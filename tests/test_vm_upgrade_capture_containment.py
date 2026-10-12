"""Authority containment for VM-upgrade freeze captures."""

from tests import _b09_control_seams as control_seams

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import orchestrator.main
from orchestrator.services.vm_remote_operation import VMRemoteOperationUnavailable
from orchestrator.services import snapshot_service as snapshot_service_module
from orchestrator.services import vm_provisioner as vm_provisioner_module


VM_GENERATION = "11111111-1111-4111-8111-111111111111"
VM_FINGERPRINT = "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"


def _contract(backend: str) -> dict:
    return {
        "version": 1,
        "requested_backend": backend,
        "assigned_backend": backend,
        "assignment_source": "test",
    }


def _snapshot_service() -> MagicMock:
    service = MagicMock()
    service.is_available = True
    service.capture_vm_snapshot = AsyncMock(return_value=True)
    return service


class _Lease:
    def __init__(self, *, host: str = "100.64.0.8") -> None:
        self.identity = SimpleNamespace(
            ssh_host=host,
            ssh_port=22,
            ssh_host_key_fingerprint=VM_FINGERPRINT,
        )
        self.revalidate = AsyncMock(return_value=(host, 22, VM_FINGERPRINT))

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


@pytest.mark.asyncio
async def test_k8s_same_ip_successor_is_not_captured():
    service = _snapshot_service()
    job = {
        "id": "job-k8s",
        "config_name": "worker_base",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            "_workspace_contract": _contract("sandbox"),
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.5",
                "_runtime_incarnation": VM_GENERATION,
            },
        },
    }

    with patch.object(snapshot_service_module, "snapshot_service", service):
        assert not await control_seams.capture_workspace_snapshot_for_freeze(
            job, job["id"]
        )

    service.capture_vm_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_vm_uses_one_exact_remote_operation_lease():
    service = _snapshot_service()
    lease = _Lease()
    claim = AsyncMock(return_value=lease)
    job = {
        "id": "job-vm",
        "config_name": "worker_base",
        "config_override": {"workspace": {"backend": "vm"}},
        "context": {
            "_workspace_contract": _contract("vm"),
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "pod_ip": "10.0.0.5",
                "_runtime_incarnation": VM_GENERATION,
            },
            "vm": {
                "status": "ready",
                "ssh_host": "100.64.0.8",
                "ssh_port": 22,
            },
        },
    }

    with (
        patch.object(snapshot_service_module, "snapshot_service", service),
        patch(
            "orchestrator.services.vm_remote_operation.claim_vm_remote_operation", claim
        ),
    ):
        assert await control_seams.capture_workspace_snapshot_for_freeze(job, job["id"])

    claim.assert_awaited_once_with(
        db=orchestrator.main.app.state.resources.postgres_db,
        provisioner=vm_provisioner_module.vm_provisioner,
        owner_id="job-vm",
        owner_kind="job",
        operation_kind="snapshot_capture",
    )
    service.capture_vm_snapshot.assert_awaited_once_with(
        job_id="job-vm",
        ssh_host="100.64.0.8",
        ssh_port=22,
        source_type="vm",
        agent_config="worker_base",
        expected_host_key_fingerprint=VM_FINGERPRINT,
        capture_authority=service.capture_vm_snapshot.await_args.kwargs[
            "capture_authority"
        ],
    )


@pytest.mark.asyncio
async def test_vm_claim_refusal_sends_no_snapshot_bytes():
    service = _snapshot_service()
    job = {
        "id": "job-vm",
        "config_name": "worker_base",
        "config_override": {"workspace": {"backend": "vm"}},
        "context": {
            "_workspace_contract": _contract("vm"),
            "vm": {"status": "ready", "ssh_host": "100.64.0.8", "ssh_port": 22},
        },
    }

    with (
        patch.object(snapshot_service_module, "snapshot_service", service),
        patch(
            "orchestrator.services.vm_remote_operation.claim_vm_remote_operation",
            AsyncMock(side_effect=VMRemoteOperationUnavailable("dark")),
        ),
    ):
        assert not await control_seams.capture_workspace_snapshot_for_freeze(
            job, job["id"]
        )

    service.capture_vm_snapshot.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_local_container_capture_remains_available():
    service = _snapshot_service()
    job = {
        "id": "job-local",
        "config_name": "worker_base",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            "_workspace_contract": _contract("sandbox"),
            "workspace_container": {
                "status": "ready",
                "provisioner": "docker",
                "host": "workspace-1",
                "port": 30022,
            },
        },
    }

    with patch.object(snapshot_service_module, "snapshot_service", service):
        assert await control_seams.capture_workspace_snapshot_for_freeze(job, job["id"])

    service.capture_vm_snapshot.assert_awaited_once_with(
        job_id="job-local",
        ssh_host="workspace-1",
        ssh_port=30022,
        source_type="pod",
        agent_config="worker_base",
    )
