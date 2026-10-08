"""The in-pod plane's chart switch (connector drivers D7, spike prototype).

``connectors.inPodPlane`` is off by default and then reaches nothing; on, it
hands the orchestrator both sidecar images pinned by digest, and it refuses
to render with either image's repository or digest missing (the opener runs
privileged, as the shim and the MCP front are pinned).
"""

from __future__ import annotations

import subprocess

import pytest

from tests.test_connector_service_hosting_helm import ROOT, orchestrator_env, render

OPENER_DIGEST = "sha256:" + "7" * 64
RCLONE_DIGEST = "sha256:" + "8" * 64
ON = "connectors.inPodPlane.enabled=true"
OPENER = (
    "connectors.inPodPlane.opener.image.repository=registry.example/srw-fuse-opener"
)
RCLONE = (
    "connectors.inPodPlane.rclone.image.repository=registry.example/srw-cloud-mount"
)
OPENER_PIN = f"connectors.inPodPlane.opener.image.digest={OPENER_DIGEST}"
RCLONE_PIN = f"connectors.inPodPlane.rclone.image.digest={RCLONE_DIGEST}"


def test_off_by_default_nothing_reaches_the_orchestrator():
    env = orchestrator_env(render())
    assert "CONNECTOR_IN_POD_OPENER_IMAGE" not in env
    assert "CONNECTOR_IN_POD_RCLONE_IMAGE" not in env


def test_on_the_orchestrator_gets_both_images_by_digest():
    env = orchestrator_env(
        render(
            ON,
            OPENER,
            RCLONE,
            OPENER_PIN,
            RCLONE_PIN,
            "connectors.inPodPlane.opener.image.tag=1.0.0",
        )
    )
    # srw.imageRef: a digest replaces the tag.
    assert env["CONNECTOR_IN_POD_OPENER_IMAGE"] == (
        f"registry.example/srw-fuse-opener@{OPENER_DIGEST}"
    )
    assert env["CONNECTOR_IN_POD_RCLONE_IMAGE"] == (
        f"registry.example/srw-cloud-mount@{RCLONE_DIGEST}"
    )


@pytest.mark.parametrize(
    ("missing", "named"),
    [
        (OPENER, "opener"),
        (RCLONE, "rclone"),
        (OPENER_PIN, "opener"),
        (RCLONE_PIN, "rclone"),
    ],
)
def test_on_without_an_image_or_its_digest_fails_to_render(missing, named):
    settings = [ON, OPENER, RCLONE, OPENER_PIN, RCLONE_PIN]
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
    assert (
        f"connectors.inPodPlane.enabled needs connectors.inPodPlane.{named}.image"
        in result.stderr
    )
