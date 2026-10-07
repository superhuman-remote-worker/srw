"""Admission-only rollout gate reaches Core and replaces every consumer."""

import pytest

from orchestrator.services.vm_job_cancel_retention import (
    cancel_retention_admission_enabled,
)
from tests.test_helm_vm_workspace_recovery import _env, _orchestrator, _stateless_agent
from tests.test_manifest_hosting_helm import render


@pytest.mark.parametrize(
    "environ,want",
    [
        ({}, False),
        ({"VM_JOB_CANCEL_RETENTION_ENABLED": "false"}, False),
        ({"VM_JOB_CANCEL_RETENTION_ENABLED": "true"}, True),
    ],
)
def test_cancel_retention_admission_flag_defaults_closed(environ, want):
    assert cancel_retention_admission_enabled(environ) is want


def test_malformed_cancel_retention_admission_flag_fails_closed():
    with pytest.raises(ValueError):
        cancel_retention_admission_enabled({"VM_JOB_CANCEL_RETENTION_ENABLED": "yes"})


def test_cancel_retention_gate_reaches_core_and_replaces_all_consumers():
    default = render("agent.stateless.enabled=true")
    enabled = render(
        "agent.stateless.enabled=true", "orchestrator.vmJobCancelRetention.enabled=true"
    )
    disabled = render(
        "agent.stateless.enabled=true",
        "orchestrator.vmJobCancelRetention.enabled=false",
        "orchestrator.vmWorkspaceRecovery.enabled=true",
    )
    key = "VM_JOB_CANCEL_RETENTION_ENABLED"
    assert _env(default, _orchestrator(default))[key] == "false"
    assert _env(enabled, _orchestrator(enabled))[key] == "true"
    assert _env(disabled, _orchestrator(disabled))[key] == "false"
    for select in (_orchestrator, _stateless_agent):
        annotations = [
            select(d)["spec"]["template"]["metadata"]["annotations"]
            for d in (default, enabled, disabled)
        ]
        assert (
            annotations[0]["checksum/vm-job-cancel-retention"]
            != annotations[1]["checksum/vm-job-cancel-retention"]
        )
        assert (
            annotations[0]["checksum/vm-job-cancel-retention"]
            == annotations[2]["checksum/vm-job-cancel-retention"]
        )
