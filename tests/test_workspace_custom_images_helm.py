"""Custom workspace image policy reaches the orchestrator."""

import shutil
import subprocess

import pytest

from tests.test_helm_vm_workspace_recovery import _env, _orchestrator
from tests.test_manifest_hosting_helm import render

pytestmark = pytest.mark.skipif(shutil.which("helm") is None, reason="Helm is absent")


def test_defaults_keep_custom_images_unprivileged():
    documents = render()
    env = _env(documents, _orchestrator(documents))
    assert env["WORKSPACE_TRUSTED_IMAGE_REPOSITORIES"] == "[]"
    assert env["WORKSPACE_CUSTOM_IMAGES_PRIVILEGED"] == "false"
    assert env["WORKSPACE_IMAGE_PULL_TIMEOUT_SECONDS"] == "600"


def test_operator_settings_are_rendered():
    documents = render(
        "workspace.images.trustedRepositories[0]=ghcr.io/org/srw-workspace-minimal",
        "workspace.customImages.privileged=true",
        "workspace.imagePullTimeoutSeconds=900",
    )
    env = _env(documents, _orchestrator(documents))
    assert env["WORKSPACE_TRUSTED_IMAGE_REPOSITORIES"] == (
        '["ghcr.io/org/srw-workspace-minimal"]'
    )
    assert env["WORKSPACE_CUSTOM_IMAGES_PRIVILEGED"] == "true"
    assert env["WORKSPACE_IMAGE_PULL_TIMEOUT_SECONDS"] == "900"


@pytest.mark.parametrize("value", ["30", "7200", "1.5"])
def test_pull_timeout_rejects_out_of_range_values(value):
    with pytest.raises(subprocess.CalledProcessError):
        render(f"workspace.imagePullTimeoutSeconds={value}")
