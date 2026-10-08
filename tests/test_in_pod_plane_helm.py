"""The in-pod plane's chart switch (connector drivers D7, spike prototype).

``connectors.inPodPlane`` is off by default and then reaches nothing; on, it
hands the orchestrator both sidecar images, and it refuses to render with
either missing.
"""

from __future__ import annotations

import subprocess

import pytest

from tests.test_connector_service_hosting_helm import ROOT, orchestrator_env, render

DIGEST = "sha256:" + "7" * 64
ON = "connectors.inPodPlane.enabled=true"
OPENER = (
    "connectors.inPodPlane.opener.image.repository=registry.example/srw-fuse-opener"
)
RCLONE = (
    "connectors.inPodPlane.rclone.image.repository=registry.example/srw-cloud-mount"
)


def test_off_by_default_nothing_reaches_the_orchestrator():
    env = orchestrator_env(render())
    assert "CONNECTOR_IN_POD_OPENER_IMAGE" not in env
    assert "CONNECTOR_IN_POD_RCLONE_IMAGE" not in env


def test_on_the_orchestrator_gets_both_images():
    env = orchestrator_env(
        render(
            ON,
            OPENER,
            RCLONE,
            "connectors.inPodPlane.opener.image.tag=1.0.0",
            f"connectors.inPodPlane.opener.image.digest={DIGEST}",
        )
    )
    assert env["CONNECTOR_IN_POD_OPENER_IMAGE"] == (
        f"registry.example/srw-fuse-opener:1.0.0@{DIGEST}"
    )
    assert env["CONNECTOR_IN_POD_RCLONE_IMAGE"] == (
        "registry.example/srw-cloud-mount:latest"
    )


@pytest.mark.parametrize("missing", [OPENER, RCLONE])
def test_on_without_an_image_fails_to_render(missing):
    settings = [ON, OPENER, RCLONE]
    settings.remove(missing)
    command = [
        "helm",
        "template",
        "srw",
        str(ROOT / "helm"),
        "-n",
        "srw",
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
    ]
    for setting in settings:
        command += ["--set", setting]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    assert result.returncode != 0
    assert "connectors.inPodPlane.enabled needs" in result.stderr
