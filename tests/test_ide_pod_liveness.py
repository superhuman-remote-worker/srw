"""The real IDE provisioner must supply the restore tail's exact drift probe."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kubernetes.client.exceptions import ApiException

from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests.test_container_provisioner import _owned_pod

JOB = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
RUNTIME = "11111111-1111-4111-8111-111111111111"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition,expected",
    [
        ("running", True),
        ("pending", True),
        ("absent", False),
        ("terminal", False),
        ("replacement", False),
        ("terminating", None),
        ("foreign_owner", None),
        ("foreign_component", None),
        ("api_error", None),
        ("unavailable", None),
        ("unknown", None),
        ("unproved_terminal", None),
    ],
)
async def test_real_ide_pod_liveness_preserves_exact_identity(condition, expected):
    provisioner = ContainerProvisioner()
    provisioner._k8s_available = condition != "unavailable"
    provisioner._core_api = MagicMock()
    owner = WorkspaceOwner.job(JOB)
    pod = _owned_pod(
        owner,
        uid=RUNTIME,
        namespace=provisioner._namespace,
        component="ide-session",
        pod_name=f"ide-{JOB[:12]}",
    )
    if condition == "pending":
        pod.status.phase = "Pending"
    elif condition in {"terminal", "unproved_terminal"}:
        pod.status.phase = "Succeeded"
        pod.status.init_container_statuses = []
        pod.status.container_statuses = (
            [SimpleNamespace(state=SimpleNamespace(terminated=SimpleNamespace()))]
            if condition == "terminal"
            else []
        )
    elif condition == "replacement":
        pod.metadata.uid = "22222222-2222-4222-8222-222222222222"
    elif condition == "terminating":
        pod.metadata.deletion_timestamp = "2026-09-28T00:00:00Z"
    elif condition == "foreign_owner":
        pod.metadata.labels[owner.label_key] = "different-owner"
    elif condition == "foreign_component":
        pod.metadata.labels["srw/component"] = "job-workspace"
    elif condition == "unknown":
        pod.status.phase = "Unknown"

    read = provisioner._core_api.read_namespaced_pod
    read.return_value = pod
    if condition in {"absent", "api_error"}:
        read.side_effect = ApiException(status=404 if condition == "absent" else 503)

    assert (
        await provisioner.ide_pod_live(JOB, expected_runtime_incarnation=RUNTIME)
        is expected
    )
    if condition == "unavailable":
        read.assert_not_called()
    else:
        assert read.call_args.kwargs["name"] == f"ide-{JOB[:12]}"
        assert read.call_args.kwargs["namespace"] == provisioner._namespace
