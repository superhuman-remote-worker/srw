"""Exact Kubernetes SSH identity for pinned worker/session delivery."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests import _b09_control_seams as control_seams

# R1.B06: these handlers moved to services/thread_config_update with their
# routes in routers/thread_config. main's dependency factory still reads
# main's attributes at call time, so the patches below keep steering what
# they steered before.
from orchestrator.services import thread_config_update  # noqa: E402
from fastapi import HTTPException

from orchestrator import main
from orchestrator.services import thread_workspace_delivery
from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation
from orchestrator.services.container_provisioner import WorkspaceRuntimeAuthorityError
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.schemas import thread_config as thread_config_module
from orchestrator.security import access as access_module
from orchestrator.services import container_provisioner as container_provisioner_module


JOB_ID = "11111111-1111-4111-8111-111111111111"
THREAD_ID = "22222222-2222-4222-8222-222222222222"
RUNTIME_A = "33333333-3333-4333-8333-333333333333"
RUNTIME_B = "44444444-4444-4444-8444-444444444444"
BACKING = "55555555-5555-4555-8555-555555555555"
BINDING_GENERATION = "66666666-6666-4666-8666-666666666666"
FINGERPRINT = "SHA256:" + "A" * 43


def _attestation(runtime: str = RUNTIME_A) -> WorkspaceRuntimeAttestation:
    return WorkspaceRuntimeAttestation(
        backing_id=f"k8s-pvc:default:{BACKING}",
        workspace_generation=BACKING,
        runtime_incarnation=runtime,
        ssh_host_key_fingerprint=FINGERPRINT,
        host=f"workspace-job-{JOB_ID}.default.svc.cluster.local",
        pod_ip="10.42.0.9",
        port=30022,
    )


def _job() -> dict:
    attested = _attestation()
    return {
        "id": JOB_ID,
        "execution_lane": "pinned",
        "config_override": {"workspace": {"backend": "sandbox"}},
        "context": {
            "_workspace_contract": {
                "version": 1,
                "requested_backend": "sandbox",
                "assigned_backend": "sandbox",
                "assignment_source": "test",
            },
            "workspace_container": {
                "status": "ready",
                "provisioner": "k8s",
                "host": attested.host,
                "pod_ip": attested.pod_ip,
                "port": attested.port,
                "_runtime_incarnation": RUNTIME_A,
                "_legacy_k8s_runtime_adoption": {
                    "version": 1,
                    "runtime_incarnation": RUNTIME_A,
                    "workspace_generation": BACKING,
                    "ssh_host_key_fingerprint": FINGERPRINT,
                },
            },
        },
    }


def _thread() -> tuple[dict, dict, dict]:
    attested = _attestation()
    workspace = {
        "status": "ready",
        "provisioner": "k8s",
        "pod_ip": attested.pod_ip,
        "port": attested.port,
        "_runtime_incarnation": RUNTIME_A,
        "_canvas_workspace_generation": BINDING_GENERATION,
    }
    binding = {
        "kind": "remote",
        "generation": BINDING_GENERATION,
        "backing_id": attested.backing_id,
        "ssh_host_key_fingerprint": FINGERPRINT,
    }
    metadata = {
        "config_override": {"workspace": {"backend": "sandbox"}},
        "workspace_container": workspace,
        "_workspace_binding": binding,
    }
    return (
        {
            "id": THREAD_ID,
            "execution_lane": "pinned",
            "metadata": metadata,
        },
        workspace,
        binding,
    )


@pytest.mark.asyncio
async def test_pinned_job_attestation_replaces_endpoint_with_exact_runtime():
    with patch.object(
        container_provisioner_module.container_provisioner,
        "attest_workspace_runtime",
        AsyncMock(return_value=_attestation()),
    ):
        exact_job, authority = await control_seams.attest_pinned_k8s_job_workspace(
            _job()
        )

    assert authority is not None
    assert authority.attestation.runtime_incarnation == RUNTIME_A
    workspace = exact_job["context"]["workspace_container"]
    assert workspace["pod_ip"] == "10.42.0.9"
    assert workspace["host"] == _attestation().host


@pytest.mark.asyncio
async def test_pinned_job_same_ip_successor_is_refused_before_delivery():
    with (
        patch.object(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(return_value=_attestation(RUNTIME_B)),
        ),
        pytest.raises(
            WorkspaceRuntimeAuthorityError,
            match="runtime changed before delivery",
        ),
    ):
        await control_seams.attest_pinned_k8s_job_workspace(_job())


@pytest.mark.asyncio
async def test_pinned_thread_same_ip_successor_is_refused_before_key_payload():
    thread, workspace, binding = _thread()
    with (
        patch.object(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(return_value=_attestation(RUNTIME_B)),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
        pytest.raises(HTTPException) as refused,
    ):
        await thread_workspace_delivery.attest_pinned_thread_k8s_workspace(
            THREAD_ID,
            thread,
            thread["metadata"],
            workspace,
            binding,
            "sandbox",
            dependencies=preparation_composition.thread_workspace_delivery_dependencies(
                main.app.state.resources
            ),
        )

    assert refused.value.status_code == 409


@pytest.mark.asyncio
async def test_pinned_thread_positive_attestation_binds_backing_and_host_key():
    thread, workspace, binding = _thread()
    with (
        patch.object(
            container_provisioner_module.container_provisioner,
            "attest_workspace_runtime",
            AsyncMock(return_value=_attestation()),
        ),
        patch.object(
            main.app.state.resources.postgres_db,
            "get_thread",
            AsyncMock(return_value=thread),
        ),
    ):
        result = await thread_workspace_delivery.attest_pinned_thread_k8s_workspace(
            THREAD_ID,
            thread,
            thread["metadata"],
            workspace,
            binding,
            "sandbox",
            dependencies=preparation_composition.thread_workspace_delivery_dependencies(
                main.app.state.resources
            ),
        )

    assert result == _attestation()


@pytest.mark.asyncio
async def test_vm_hot_upgrade_keeps_separate_authority_path():
    expected = {"status": "provisioning", "thread_id": THREAD_ID}
    with (
        patch.object(access_module, "require_internal", AsyncMock()),
        # R1.B06: the VM fork is a call *inside* services/thread_config_update,
        # so the double has to live there — patching main would be inert.
        patch.object(
            thread_config_update,
            "agent_upgrade_thread_to_vm",
            AsyncMock(return_value=expected),
        ) as upgrade_vm,
    ):
        result = await thread_config_update.agent_upgrade_thread_to_workspace(
            MagicMock(),
            THREAD_ID,
            thread_config_module.ThreadWorkspaceUpgradeRequest(target_tier="vm"),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

    assert result == expected
    upgrade_vm.assert_awaited_once()
