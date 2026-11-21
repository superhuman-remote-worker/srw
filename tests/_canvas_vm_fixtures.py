"""Shared same-cluster VM rows and controller proofs for Canvas VM tests.

The VM context mirrors what ``vm_readiness`` and the VM provisioner publish for
a ready, controller-authenticated thread VM; the proof mirrors
``VMProvisioner.attest_workspace_runtime``'s ``WorkspaceRuntimeAttestation``.
"""

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from typing import Any

VM_THREAD_ID = "a3333333-3333-3333-3333-333333333333"
VM_PROVISION_GENERATION = "55555555-5555-4555-8555-555555555555"
VM_LAUNCHER = "66666666-6666-4666-8666-666666666666"
VM_ROOTDISK = "99999999-9999-4999-8999-999999999999"
VM_VMI = "88888888-8888-4888-8888-888888888888"
VM_FINGERPRINT = "SHA256:" + "A" * 43
VM_HOST = "10.42.1.23"

# A restore onto the kept rootdisk: everything about the runtime is new.
RESTORED_PROVISION_GENERATION = "77777777-7777-4777-8777-777777777777"
RESTORED_LAUNCHER = "12121212-1212-4212-8212-121212121212"
RESTORED_FINGERPRINT = "SHA256:" + "B" * 43
RESTORED_HOST = "10.42.7.9"


def vm_context(**overrides: Any) -> dict[str, Any]:
    context: dict[str, Any] = {
        "status": "ready",
        "provision_generation": VM_PROVISION_GENERATION,
        "identity_authenticated": True,
        "identity_provision_generation": VM_PROVISION_GENERATION,
        "vm_uid": "vm-uid",
        "vmi_uid": VM_VMI,
        "rootdisk_pvc_uid": VM_ROOTDISK,
        "active_pod_uid": VM_LAUNCHER,
        "ssh_registration_id": "registration-1",
        "ssh_ready_source": "provisioner_probe",
        "ssh_host_key_fingerprint": VM_FINGERPRINT,
        "ssh_host": VM_HOST,
        "ssh_port": 22,
    }
    context.update(overrides)
    return context


def restored_vm_context(**overrides: Any) -> dict[str, Any]:
    """The same Session after a VM restore onto its kept rootdisk."""

    return vm_context(
        provision_generation=RESTORED_PROVISION_GENERATION,
        identity_provision_generation=RESTORED_PROVISION_GENERATION,
        active_pod_uid=RESTORED_LAUNCHER,
        ssh_host_key_fingerprint=RESTORED_FINGERPRINT,
        ssh_host=RESTORED_HOST,
        **overrides,
    )


def vm_thread(
    vm: dict[str, Any] | None = None,
    *,
    backend: str = "vm",
    user_id: str = "user-1",
    thread_id: str = VM_THREAD_ID,
    extra_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "config_override": {"workspace": {"backend": backend}},
        # Every thread carries a git-only workspace_container (_setup_gitea).
        "workspace_container": {"git_remote_url": "http://gitea/repo"},
        "vm": deepcopy(vm) if vm is not None else vm_context(),
    }
    metadata.update(extra_metadata or {})
    return {
        "id": thread_id,
        "user_id": user_id,
        "execution_lane": "pinned",
        "status": "active",
        "metadata": metadata,
    }


def proof_for(vm: dict[str, Any], **overrides: Any) -> SimpleNamespace:
    """A controller attestation that proves exactly ``vm``."""

    values: dict[str, Any] = {
        "backing_id": f"k8s-vmi:{vm['active_pod_uid']}",
        "workspace_generation": vm["provision_generation"],
        "runtime_incarnation": vm["active_pod_uid"],
        "ssh_host_key_fingerprint": vm["ssh_host_key_fingerprint"],
        "host": vm["ssh_host"],
        "pod_ip": vm["ssh_host"],
        "port": int(vm["ssh_port"]),
        "vm_uid": vm["vm_uid"],
        "launcher_pod_uid": vm["active_pod_uid"],
        "vmi_uid": vm.get("vmi_uid"),
        "rootdisk_pvc_uid": vm["rootdisk_pvc_uid"],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class FakeVMProvisioner:
    """Answers ``attest_workspace_runtime`` from a mutable thread row."""

    def __init__(self, thread: dict[str, Any]) -> None:
        self.thread = thread
        self.calls: list[tuple[str, str]] = []
        self.error: BaseException | None = None
        self.proof_overrides: dict[str, Any] = {}

    async def attest_workspace_runtime(self, owner_id: str, *, entity_type: str):
        self.calls.append((owner_id, entity_type))
        if self.error is not None:
            raise self.error
        return proof_for(self.thread["metadata"]["vm"], **self.proof_overrides)


__all__ = [
    "FakeVMProvisioner",
    "RESTORED_FINGERPRINT",
    "RESTORED_HOST",
    "RESTORED_LAUNCHER",
    "RESTORED_PROVISION_GENERATION",
    "VM_FINGERPRINT",
    "VM_HOST",
    "VM_LAUNCHER",
    "VM_PROVISION_GENERATION",
    "VM_ROOTDISK",
    "VM_THREAD_ID",
    "proof_for",
    "restored_vm_context",
    "vm_context",
    "vm_thread",
]
