"""Storage progress budget is configurable and positive in deployed pods."""

import shutil
import subprocess

import pytest

from tests.test_helm_vm_workspace_recovery import _env, _orchestrator
from tests.test_manifest_hosting_helm import render

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")


def test_rootdisk_stall_budget_default_and_override():
    default = render()
    tuned = render("orchestrator.vmProvisioning.rootdiskStallTimeoutSeconds=5400")
    assert (
        _env(default, _orchestrator(default))["VM_ROOTDISK_STALL_TIMEOUT_S"] == "2700"
    )
    assert _env(tuned, _orchestrator(tuned))["VM_ROOTDISK_STALL_TIMEOUT_S"] == "5400"
    assert (
        _orchestrator(default)["spec"]["template"]
        != _orchestrator(tuned)["spec"]["template"]
    )


@pytest.mark.parametrize("value", ["0", "-1", "1.5"])
def test_rootdisk_stall_budget_rejects_nonpositive_or_fractional_values(value):
    with pytest.raises(subprocess.CalledProcessError):
        render(f"orchestrator.vmProvisioning.rootdiskStallTimeoutSeconds={value}")


def test_creation_retry_admission_defaults_off_and_can_be_enabled():
    default = render()
    enabled = render("orchestrator.vmProvisioning.creationRetryEnabled=true")
    assert _env(default, _orchestrator(default))["VM_CREATION_RETRY_ENABLED"] == "false"
    assert _env(enabled, _orchestrator(enabled))["VM_CREATION_RETRY_ENABLED"] == "true"
