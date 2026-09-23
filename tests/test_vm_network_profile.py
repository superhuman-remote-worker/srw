"""The optional retained-disk network profile reaches the real VM renderer."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tests.test_vm_resource_template import shipped_template
from vm_controller import controller as settings
from vm_controller.creation_configuration import resolve_creation_configuration
from tests.test_vm_resource_manifest import final_case  # noqa: F401


IMAGE = "registry.example/srw-vm@sha256:" + "a" * 64


def _controller(monkeypatch):
    monkeypatch.setattr(settings, "VM_NODE_SELECTOR", {})
    monkeypatch.setattr(settings, "VM_TOLERATIONS", [])
    monkeypatch.setattr(settings, "_inject_ssh_host_key", lambda value: (value, "test-fingerprint"))
    controller = settings.VMController.__new__(settings.VMController)
    controller.template_text = shipped_template()
    controller.cloud_init_text = "#cloud-config"
    controller.headscale = SimpleNamespace(is_available=False)
    return controller


def _request():
    return {
        "job_id": str(uuid4()),
        "entity_type": "job",
        "provision_generation": str(uuid4()),
        "vm_image": IMAGE,
        "cpu_cores": 2,
        "memory": "512Mi",
        "disk_size": "32Gi",
        "network_tier": "internet-only",
    }


def test_explicit_profile_renders_name_only_nocloud_dhcp_and_preserves_legacy(monkeypatch):
    from shared.vm_network_profile import NETWORK_PROFILE

    controller = _controller(monkeypatch)
    request = _request()
    legacy = controller.render_template(request)
    opted = controller.render_template({**request, "network_profile": NETWORK_PROFILE})
    def cloud_volume(vm):
        return next(v["cloudInitNoCloud"] for v in vm["spec"]["template"]["spec"]["volumes"] if "cloudInitNoCloud" in v)
    assert set(cloud_volume(legacy)) == {"secretRef"}
    assert cloud_volume(opted)["secretRef"] == cloud_volume(legacy)["secretRef"]
    assert cloud_volume(opted)["networkData"] == (
        "version: 2\nethernets:\n  enp1s0:\n    match:\n      name: enp1s0\n    dhcp4: true\n    dhcp6: true\n"
    )
    assert "macaddress" not in cloud_volume(opted)["networkData"]
    assert legacy["_srwCloudInitUserData"] == "#cloud-config"
    assert "/usr/local/bin/srw-network-profile-qualification" in opted["_srwCloudInitUserData"]


@pytest.mark.asyncio
async def test_legacy_controller_create_cannot_inject_profile_without_durable_authority(monkeypatch):
    from shared.vm_network_profile import NETWORK_PROFILE

    controller = _controller(monkeypatch)
    with pytest.raises(ValueError, match="durable creation authority"):
        await controller._do_create_serialized({**_request(), "network_profile": NETWORK_PROFILE})


def test_profile_configuration_requires_explicit_immutable_compatible_image(monkeypatch):
    from shared.vm_network_profile import NETWORK_PROFILE

    controller = _controller(monkeypatch)
    request = {**_request(), "network_profile": NETWORK_PROFILE}
    monkeypatch.delenv("VM_NETWORK_PROFILE_ENABLED", raising=False)
    monkeypatch.delenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", raising=False)
    with pytest.raises(ValueError):
        resolve_creation_configuration(controller, request)
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", IMAGE)
    resolved = resolve_creation_configuration(controller, request)
    assert resolved["request"]["network_profile"] == NETWORK_PROFILE
    assert resolved["controller_configuration"]["network_profile_policy"] == {
        "version": 1, "image": IMAGE, "profile": NETWORK_PROFILE,
    }
    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "false")
    assert resolve_creation_configuration(controller, request)["request"]["network_profile"] == NETWORK_PROFILE
    changed = deepcopy(request)
    changed["vm_image"] = "registry.example/srw-vm:latest"
    with pytest.raises(ValueError):
        resolve_creation_configuration(controller, changed)


@pytest.mark.asyncio
async def test_actual_vm_effect_manifest_and_observation_bind_network_data(
    monkeypatch, final_case,  # noqa: F811
):
    from shared.vm_network_profile import NETWORK_PROFILE, NETWORK_DATA
    from shared.vm_resource_manifest import validate_final_vm_manifest
    from shared.vm_resource_admission import ResourceAdmissionError
    from shared.vm_creation_issuance import public_effect_observation
    from tests.test_vm_resource_policy import snapshot

    monkeypatch.setenv("VM_NETWORK_PROFILE_ENABLED", "true")
    monkeypatch.setenv("VM_NETWORK_PROFILE_IMAGE_ALLOWLIST", IMAGE)
    ctrl, actuator, row, intent = final_case
    request = {**row["request"], "vm_image": IMAGE, "network_profile": NETWORK_PROFILE}
    row.update(resolve_creation_configuration(ctrl, request, _resource_policy_snapshot=snapshot()))
    values = intent("vm")
    values["rootdisk_source"] = {"kind": "registry", "image": IMAGE}
    body = await actuator.body(row, values)
    validate_final_vm_manifest(
        body, template_text=ctrl.template_text, request=row["request"],
        configuration=row["controller_configuration"], effect_intent=values,
    )
    cloud = next(v["cloudInitNoCloud"] for v in body["spec"]["template"]["spec"]["volumes"] if "cloudInitNoCloud" in v)
    assert cloud["networkData"] == NETWORK_DATA
    changed = deepcopy(body)
    changed_cloud = next(v["cloudInitNoCloud"] for v in changed["spec"]["template"]["spec"]["volumes"] if "cloudInitNoCloud" in v)
    changed_cloud["networkData"] = NETWORK_DATA.replace("enp1s0", "eth0")
    with pytest.raises(ResourceAdmissionError):
        validate_final_vm_manifest(
            changed, template_text=ctrl.template_text, request=row["request"],
            configuration=row["controller_configuration"], effect_intent=values,
        )
    # The observed effect is checked again from the authenticated request even
    # if the Kubernetes admission or later mutation changed the VM body.
    changed["metadata"]["uid"] = str(uuid4())
    with pytest.raises(ValueError, match="network"):
        public_effect_observation(
            values,
            {"metadata": {"namespace": changed["metadata"]["namespace"]}},
            {"outcome": "observed", "object": changed},
            rootdisk={"outcome": "observed", "name": "agent-vm-" + row["job_id"] + "-rootdisk", "pvc_uid": str(uuid4())},
            cloud_init={"outcome": "observed", "name": "agent-vm-" + row["job_id"] + "-cloudinit", "uid": str(uuid4()), "ssh_host_key_fingerprint": "SHA256:" + "A" * 43},
            network_profile=NETWORK_PROFILE,
        )


def test_guest_probe_requires_selected_name_only_networkd_rule():
    from shared.vm_network_probe_guest import parse_networkd_rule

    status = "  Network File: /run/systemd/network/10-netplan-enp1s0.network\n"
    rule = "[Match]\nName=enp1s0\n\n[Network]\nDHCP=yes\n"
    assert parse_networkd_rule(status, rule) == "/run/systemd/network/10-netplan-enp1s0.network"
    assert parse_networkd_rule(status, rule.replace("Name=enp1s0", "MACAddress=c6:87:08:b1:b8:38")) is None
    assert parse_networkd_rule(status, rule.replace("Name=enp1s0", "Name=enp1s0\nDriver=virtio_net")) is None
    assert parse_networkd_rule(status, rule.replace("DHCP=yes", "DHCP=no")) is None
    assert parse_networkd_rule(status.replace("/run/systemd/network/", "/tmp/"), rule) is None


def test_guest_probe_collects_cache_link_without_reading_private_cache(tmp_path):
    import json
    from shared.vm_network_probe_guest import collect

    def write(path, contents):
        file = tmp_path / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(contents)
    write("proc/sys/kernel/random/boot_id", "00000000-0000-4000-8000-000000000001")
    write("etc/machine-id", "41" * 16)
    write("etc/resolv.conf", "nameserver 10.0.2.3")
    write("var/lib/cloud/data/instance-id", "instance-one")
    write("run/systemd/network/10-netplan-enp1s0.network", "[Match]\nName=enp1s0\n[Network]\nDHCP=yes\n")
    (tmp_path / "var/lib/cloud/instance").symlink_to("instances/instance-one")
    # No readable instances tree is provided. The symlink target is enough to
    # distinguish the active cache identity without walking private files.
    def run(*args):
        if args[:2] == ("ip", "-j") and args[2] == "address":
            return json.dumps([{"ifname": "enp1s0", "address": "02:00:00:00:00:01", "addr_info": [{"local": "10.0.2.15"}]}])
        if args[:2] == ("ip", "-j") and args[2] == "route":
            return json.dumps([{"dst": "default", "gateway": "10.0.2.2"}])
        if args[0] == "networkctl":
            return "Network File: /run/systemd/network/10-netplan-enp1s0.network"
        return None
    evidence = collect("fresh", root=tmp_path, run=run)
    assert evidence["network_profile_rule"]["name_only_dhcp"] is True
    assert evidence["cloud_init_cached_instance_id"] == "instance-one"
    assert evidence["cloud_init_instance_id"] == "instance-one"
    assert evidence["networkd_sha256"]


@pytest.mark.asyncio
async def test_authenticated_profile_successor_rejects_stale_rule_and_cache(monkeypatch):
    from tests.test_vm_readiness import (
        complete_recovery_network, wire_recovery_qualification,
        HOST_KEY_FINGERPRINT,
    )
    from orchestrator.services.vm_readiness import qualify_recovery_successor
    from shared.vm_network_profile import NETWORK_PROFILE

    successor = {
        "pod_ip": "10.42.0.90",
        "vmi_uid": "00000000-0000-4000-8000-000000000042",
        "launcher_uid": "00000000-0000-4000-8000-000000000043",
        "interface_mac": "02:00:00:00:00:41",
    }
    telemetry = complete_recovery_network()
    telemetry["interfaces"][0]["ifname"] = "enp1s0"
    telemetry["cloud_init_instance_id"] = "instance-one"
    telemetry["cloud_init_cached_instance_id"] = "instance-one"
    telemetry["network_profile_rule"] = {
        "kind": "networkd-name-dhcp-v1", "interface": "enp1s0",
        "name_only_dhcp": True, "network_file_sha256": "a" * 64,
    }
    telemetry["networkd_sha256"] = {
        "/run/systemd/network/10-netplan-enp1s0.network": "a" * 64,
    }
    wire_recovery_qualification(monkeypatch, telemetry)
    assert await qualify_recovery_successor(
        successor, host_key_fingerprint=HOST_KEY_FINGERPRINT,
        network_profile=NETWORK_PROFILE,
    ) is not None
    telemetry["network_profile_rule"]["name_only_dhcp"] = False
    wire_recovery_qualification(monkeypatch, telemetry)
    assert await qualify_recovery_successor(
        successor, host_key_fingerprint=HOST_KEY_FINGERPRINT,
        network_profile=NETWORK_PROFILE,
    ) is None
    telemetry["network_profile_rule"]["name_only_dhcp"] = True
    telemetry["cloud_init_cached_instance_id"] = "other"
    wire_recovery_qualification(monkeypatch, telemetry)
    assert await qualify_recovery_successor(
        successor, host_key_fingerprint=HOST_KEY_FINGERPRINT,
        network_profile=NETWORK_PROFILE,
    ) is None


@pytest.mark.asyncio
async def test_first_boot_readiness_requires_profile_rule_and_records_exact_disk(monkeypatch):
    from unittest.mock import AsyncMock
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation
    from orchestrator.services.vm_readiness import VMReadinessService
    from tests.test_vm_readiness import FakeDB, FakeProvisioner, candidate, GENERATION, HOST_KEY_FINGERPRINT
    from shared.vm_network_profile import NETWORK_PROFILE

    vm_uid, pvc_uid = str(uuid4()), str(uuid4())
    vmi_uid, launcher_uid = str(uuid4()), str(uuid4())
    row = candidate(creation_preflight={"request": {"network_profile": NETWORK_PROFILE}})
    status = {
        "ready": True, "phase": "Running", "pod_ip": "10.42.0.10",
        "active_pod_uid": launcher_uid, "vmi_uid": vmi_uid,
        "interface_mac": "02:00:00:00:00:41",
        "vm_uid": vm_uid, "rootdisk_pvc_uid": pvc_uid,
    }
    attestation = WorkspaceRuntimeAttestation(
        backing_id="k8s-vmi:" + launcher_uid,
        workspace_generation=GENERATION, runtime_incarnation=launcher_uid,
        ssh_host_key_fingerprint=HOST_KEY_FINGERPRINT,
        host="10.42.0.10", pod_ip="10.42.0.10", port=22,
        vm_uid=vm_uid, vmi_uid=vmi_uid, launcher_pod_uid=launcher_uid,
        rootdisk_pvc_uid=pvc_uid,
    )
    monkeypatch.setattr("orchestrator.services.vm_readiness.wait_for_agent_ssh", AsyncMock(return_value=(True, 1, None)))
    monkeypatch.setattr("orchestrator.services.vm_readiness.seed_ide_config_for_user", AsyncMock(return_value=True))
    qualifier = AsyncMock(return_value=None)
    monkeypatch.setattr("orchestrator.services.vm_readiness.qualify_recovery_successor", qualifier)
    db = FakeDB(jobs=[row])
    provisioner = FakeProvisioner(status)
    provisioner.attest_workspace_runtime = AsyncMock(return_value=attestation)
    await VMReadinessService(db, provisioner, trigger_dispatch=lambda: None).run_cycle()
    assert db.promotions == []
    assert qualifier.await_args.kwargs["network_profile"] == NETWORK_PROFILE
    qualifier.return_value = {
        "guest_boot_id": str(uuid4()),
        "guest_network": {
            "cloud_init_instance_id": "instance-one",
            "cloud_init_cached_instance_id": "instance-one",
            "network_profile_rule": {"network_file_sha256": "a" * 64},
        },
    }
    db = FakeDB(jobs=[row])
    provisioner = FakeProvisioner(status)
    provisioner.attest_workspace_runtime = AsyncMock(return_value=attestation)
    await VMReadinessService(db, provisioner, trigger_dispatch=lambda: None).run_cycle()
    evidence = db.promotions[-1][3]["network_profile_evidence"]
    assert evidence["pvc_uid"] == pvc_uid
    assert evidence["vm_uid"] == vm_uid
    assert evidence["vmi_uid"] == vmi_uid
    assert evidence["launcher_uid"] == launcher_uid
    assert evidence["name_only_dhcp"] is True
