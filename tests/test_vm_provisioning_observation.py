"""Controller phase evidence binds the actual Kubernetes ownership chain."""

import copy

import pytest

from vm_controller.provisioning_observation import build_provisioning_observation

OWNER = "00000000-0000-4000-8000-000000000001"
GENERATION = "00000000-0000-4000-8000-000000000002"
VM_UID = "00000000-0000-4000-8000-000000000003"
VMI_UID = "00000000-0000-4000-8000-000000000004"
DV_UID = "00000000-0000-4000-8000-000000000005"
PVC_UID = "00000000-0000-4000-8000-000000000006"


def metadata(name, uid):
    return {
        "name": name,
        "namespace": "srw",
        "uid": uid,
        "labels": {"srw.io/owner-kind": "job", "srw.io/owner-id": OWNER},
    }


def objects(*, phase="CloneInProgress", vmi_phase="Pending", direct=False):
    source = (
        {"persistentVolumeClaim": {"claimName": "rootdisk"}}
        if direct
        else {"dataVolume": {"name": "rootdisk"}}
    )
    volumes = [{"name": "rootdisk", **source}]
    vm = {
        "metadata": {
            **metadata(f"agent-vm-{OWNER}", VM_UID),
            "annotations": {"srw.io/provision-generation": GENERATION},
        },
        "spec": {"template": {"spec": {"volumes": volumes}}},
    }
    vmi = {
        "metadata": {
            **metadata(f"agent-vm-{OWNER}", VMI_UID),
            "ownerReferences": [
                {"kind": "VirtualMachine", "uid": VM_UID, "controller": True}
            ],
        },
        "spec": {"volumes": copy.deepcopy(volumes)},
        "status": {"phase": vmi_phase},
    }
    dv = {
        "metadata": metadata("rootdisk", DV_UID),
        "status": {"phase": phase, "progress": "12.5%"},
    }
    pvc = {
        "metadata": {
            **metadata("rootdisk", PVC_UID),
            "ownerReferences": [
                {
                    "kind": "DataVolume",
                    "name": "rootdisk",
                    "uid": DV_UID,
                    "controller": True,
                }
            ],
        },
        "status": {"phase": "Bound"},
    }
    return {"vm": vm, "vmi": vmi, "datavolume": dv, "pvc": pvc}


def observe(value):
    return build_provisioning_observation(
        **value,
        namespace="srw",
        owner_kind="job",
        owner_id=OWNER,
        generation=GENERATION,
        rootdisk_name="rootdisk",
        rootdisk_owner_kind="job",
        rootdisk_owner_id=OWNER,
    )


@pytest.mark.parametrize("vmi_phase", ["Pending", "Scheduling", "Scheduled", "Running"])
def test_exact_vmi_phase_is_preserved_without_printable_status_inference(vmi_phase):
    result = observe(objects(phase="Succeeded", vmi_phase=vmi_phase))
    assert result["vmi_phase"] == vmi_phase.lower()
    assert result["vmi_uid"] == VMI_UID
    assert result["rootdisk_pvc_uid"] == PVC_UID


@pytest.mark.parametrize(
    "phase", ["WaitForFirstConsumer", "PendingPopulation", "Paused"]
)
def test_consumer_or_checkpoint_dependency_wait_is_not_active_clone(phase):
    result = observe(objects(phase=phase))
    assert result["disk_phase"] == "waiting_for_consumer"


def test_transfer_progress_is_bounded_numeric_evidence():
    result = observe(objects())
    assert (result["disk_phase"], result["disk_progress"]) == ("transferring", 12.5)


@pytest.mark.parametrize("progress", ["N/A", "NaN%", "101%", "-1%", "bad", True, None])
def test_unusable_progress_remains_unknown(progress):
    value = objects()
    value["datavolume"]["status"]["progress"] = progress
    assert observe(value)["disk_progress"] is None


def test_direct_retained_bound_pvc_is_valid_without_datavolume():
    value = objects(direct=True, vmi_phase="Running")
    value["datavolume"] = None
    result = observe(value)
    assert result["disk_mode"] == "retained"
    assert result["rootdisk_dv_uid"] is None
    assert result["disk_phase"] == "ready"


def test_missing_vmi_is_explicit_absence_not_boot():
    value = objects()
    value["vmi"] = None
    result = observe(value)
    assert (result["vmi_uid"], result["vmi_phase"]) == (None, "absent")


def test_initial_unallocated_pvc_keeps_disk_preparation_evidence():
    value = objects(phase="Pending")
    value["pvc"] = None
    assert observe(value)["rootdisk_pvc_uid"] is None


@pytest.mark.parametrize("object_name", ["vm", "vmi", "datavolume", "pvc"])
def test_deleting_object_is_not_current_phase_authority(object_name):
    value = objects()
    value[object_name]["metadata"]["deletionTimestamp"] = "2026-09-19T00:00:00Z"
    with pytest.raises(ValueError):
        observe(value)


@pytest.mark.parametrize("object_name", ["vm", "datavolume", "pvc"])
def test_foreign_owner_labels_are_not_accepted(object_name):
    value = objects()
    value[object_name]["metadata"]["labels"]["srw.io/owner-id"] = VMI_UID
    with pytest.raises(ValueError):
        observe(value)


@pytest.mark.parametrize("object_name", ["vmi", "pvc"])
def test_owner_reference_must_bind_exact_parent_uid(object_name):
    value = objects()
    value[object_name]["metadata"]["ownerReferences"][0]["uid"] = OWNER
    with pytest.raises(ValueError):
        observe(value)


def test_vmi_using_different_disk_is_not_boot_evidence():
    value = objects(vmi_phase="Running")
    value["vmi"]["spec"]["volumes"][0]["dataVolume"]["name"] = "another-disk"
    with pytest.raises(ValueError):
        observe(value)


def test_wrong_generation_does_not_publish_phase():
    value = objects()
    value["vm"]["metadata"]["annotations"]["srw.io/provision-generation"] = OWNER
    with pytest.raises(ValueError):
        observe(value)


def test_missing_datavolume_does_not_claim_original_disk_was_lost():
    value = objects()
    value["datavolume"] = None
    result = observe(value)
    assert result["rootdisk_pvc_uid"] == PVC_UID
    assert result["disk_phase"] == "unknown"


def test_kubernetes_model_objects_are_supported():
    from kubernetes.client import (
        V1PersistentVolumeClaim,
        V1ObjectMeta,
        V1OwnerReference,
        V1PersistentVolumeClaimStatus,
    )

    value = objects(phase="Succeeded")
    value["pvc"] = V1PersistentVolumeClaim(
        metadata=V1ObjectMeta(
            name="rootdisk",
            namespace="srw",
            uid=PVC_UID,
            labels={"srw.io/owner-kind": "job", "srw.io/owner-id": OWNER},
            owner_references=[
                V1OwnerReference(
                    api_version="cdi.kubevirt.io/v1beta1",
                    kind="DataVolume",
                    name="rootdisk",
                    uid=DV_UID,
                    controller=True,
                )
            ],
        ),
        status=V1PersistentVolumeClaimStatus(phase="Bound"),
    )
    assert observe(value)["disk_phase"] == "ready"


@pytest.fixture
def status_controller(monkeypatch):
    from unittest.mock import MagicMock
    from vm_controller import controller as module

    monkeypatch.setattr(module, "VM_NAMESPACE", "srw")
    ctrl = module.VMController()
    ctrl.k8s_client = MagicMock()
    ctrl.core_api = MagicMock()
    value = objects(phase="Succeeded", vmi_phase="Running")
    disk_name = f"agent-vm-{OWNER}-rootdisk"
    value["datavolume"]["metadata"]["name"] = disk_name
    value["pvc"]["metadata"]["name"] = disk_name
    for obj in (value["vm"]["spec"]["template"], value["vmi"]):
        obj["spec"]["volumes"][0]["dataVolume"]["name"] = disk_name

    def read(**kwargs):
        return value[
            {
                "virtualmachines": "vm",
                "virtualmachineinstances": "vmi",
                "datavolumes": "datavolume",
            }[kwargs["plural"]]
        ]

    ctrl.k8s_client.get_namespaced_custom_object.side_effect = read
    ctrl.core_api.read_namespaced_persistent_volume_claim.side_effect = (
        lambda **_: value["pvc"]
    )
    return ctrl, value


@pytest.mark.asyncio
async def test_controller_status_includes_phase_evidence_without_promoting_ready(
    status_controller,
):
    ctrl, _ = status_controller
    result = await ctrl._do_status(OWNER, GENERATION)
    assert result["provisioning"]["vmi_uid"] == VMI_UID
    assert result["provisioning"]["vmi_phase"] == "running"
    assert result["ready"] is False


@pytest.mark.asyncio
async def test_phase_read_failure_is_unproven_not_object_absence(status_controller):
    ctrl, _ = status_controller
    read = ctrl.k8s_client.get_namespaced_custom_object.side_effect

    def unavailable(**kwargs):
        if kwargs["plural"] == "datavolumes":
            raise TimeoutError("private internal endpoint")
        return read(**kwargs)

    ctrl.k8s_client.get_namespaced_custom_object.side_effect = unavailable
    result = await ctrl._do_status(OWNER, GENERATION)
    assert "provisioning" not in result
    assert result["provisioning_reason"] == "vm_phase_unproven"
    assert "private internal endpoint" not in str(result)


@pytest.mark.asyncio
async def test_retained_storage_binding_with_actual_datavolume_volume_is_observed(
    status_controller,
):
    from unittest.mock import AsyncMock
    from shared.vm_workspace_storage import storage_labels, storage_name

    ctrl, value = status_controller
    binding = {
        "uid": OWNER,
        "generation": 2,
        "pvc_uid": PVC_UID,
        "owner_id": OWNER,
        "owner_kind": "job",
    }
    name = storage_name(binding)
    value["datavolume"]["metadata"]["name"] = name
    value["pvc"]["metadata"]["name"] = name
    value["vm"]["metadata"]["labels"].update(storage_labels(binding, OWNER))
    for obj in (value["vm"]["spec"]["template"], value["vmi"]):
        obj["spec"]["volumes"][0]["dataVolume"]["name"] = name
    # Keep actual volume/attachment verification; only the separate lease/PVC
    # control probe is already proven outside this phase-observation boundary.
    ctrl._retained_storage().probe = AsyncMock(return_value=PVC_UID)
    result = await ctrl._do_status(OWNER, GENERATION, workspace_storage=binding)
    assert result["provisioning"]["disk_phase"] == "ready"
    assert result["provisioning"]["rootdisk_dv_uid"] == DV_UID


@pytest.mark.parametrize(
    "phase,expected", [("Lost", "failed"), (None, "unknown"), ([], "unknown")]
)
def test_retained_disk_loss_or_unknown_state_is_not_placement_wait(phase, expected):
    value = objects(direct=True)
    value["datavolume"] = None
    value["pvc"]["status"]["phase"] = phase
    assert observe(value)["disk_phase"] == expected
