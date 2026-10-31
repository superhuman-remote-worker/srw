"""The idle job workspace Pod release setting reaches the orchestrator."""

from tests.test_helm_vm_workspace_recovery import _env, _orchestrator
from tests.test_manifest_hosting_helm import render

KEY = "WORKSPACE_IDLE_POD_RELEASE_SECONDS"


def test_idle_pod_release_defaults_to_a_day_and_can_be_disabled():
    default = render()
    disabled = render("workspace.idlePodReleaseSeconds=0")
    hourly = render("workspace.idlePodReleaseSeconds=3600")

    assert _env(default, _orchestrator(default))[KEY] == "86400"
    assert _env(disabled, _orchestrator(disabled))[KEY] == "0"
    assert _env(hourly, _orchestrator(hourly))[KEY] == "3600"
    # The release is an automatic workspace cleanup, so it stays inert until
    # cleanup reconciliation is switched on; the chart keeps that default dark.
    assert (
        _env(default, _orchestrator(default))[
            "WORKSPACE_CLEANUP_RECONCILIATION_ENABLED"
        ]
        == "false"
    )
