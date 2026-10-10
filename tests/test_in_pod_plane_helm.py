"""The in-pod plane's chart switch (connector drivers D7).

``connectors.inPodPlane`` is on by default (owner, 2026-10-10): every
installation's session workspace Pods get their cloud mounts from the
sidecars. It hands the orchestrator both sidecar images (by digest when CI
stamped one, else by tag) and its settings, grants the orchestrator create
(and only create) on Secrets in the release namespace, and warms the
supervisor image on every node. Off, none of that renders. A missing image
repository fails the render.
"""

from __future__ import annotations

import subprocess

import pytest
import yaml

from tests.test_connector_service_hosting_helm import ROOT, orchestrator_env, render

OPENER_DIGEST = "sha256:" + "7" * 64
RCLONE_DIGEST = "sha256:" + "8" * 64
OFF = "connectors.inPodPlane.enabled=false"
OPENER_PIN = f"connectors.inPodPlane.opener.image.digest={OPENER_DIGEST}"
RCLONE_PIN = f"connectors.inPodPlane.rclone.image.digest={RCLONE_DIGEST}"


def _orchestrator_role(docs: list[dict]) -> dict:
    return next(
        doc
        for doc in docs
        if doc["kind"] == "Role" and doc["metadata"]["name"].endswith("-orchestrator")
    )


def _secret_rules(docs: list[dict]) -> list[dict]:
    return [
        rule
        for rule in _orchestrator_role(docs)["rules"]
        if "secrets" in rule.get("resources", [])
    ]


def test_on_by_default_the_orchestrator_gets_both_images_and_the_settings():
    env = orchestrator_env(render())
    assert env["CONNECTOR_IN_POD_OPENER_IMAGE"] == (
        "ghcr.io/superhuman-remote-worker/srw-fuse-opener:latest"
    )
    assert env["CONNECTOR_IN_POD_RCLONE_IMAGE"] == (
        "ghcr.io/superhuman-remote-worker/srw-cloud-mount:latest"
    )
    assert env["CONNECTOR_IN_POD_CACHE_SIZE"] == "10Gi"
    assert env["CONNECTOR_IN_POD_DRAIN_SECONDS"] == "60"
    assert env["CONNECTOR_IN_POD_MAX_MOUNTS"] == "8"


def test_a_stamped_digest_replaces_the_tag():
    env = orchestrator_env(render(OPENER_PIN, RCLONE_PIN))
    assert env["CONNECTOR_IN_POD_OPENER_IMAGE"] == (
        f"ghcr.io/superhuman-remote-worker/srw-fuse-opener@{OPENER_DIGEST}"
    )
    assert env["CONNECTOR_IN_POD_RCLONE_IMAGE"] == (
        f"ghcr.io/superhuman-remote-worker/srw-cloud-mount@{RCLONE_DIGEST}"
    )


def test_the_orchestrator_may_only_create_secrets_and_only_with_the_plane():
    assert _secret_rules(render()) == [
        {"apiGroups": [""], "resources": ["secrets"], "verbs": ["create"]}
    ]
    assert _secret_rules(render(OFF)) == []


def test_off_nothing_renders():
    docs = render(OFF)
    env = orchestrator_env(docs)
    assert not [name for name in env if name.startswith("CONNECTOR_IN_POD_")]
    prewarm = [
        d
        for d in docs
        if d["kind"] == "DaemonSet" and "prewarm" in d["metadata"]["name"]
    ]
    for daemonset in prewarm:
        names = [
            c["name"] for c in daemonset["spec"]["template"]["spec"]["initContainers"]
        ]
        assert "pull-cloud-mount" not in names


def test_the_supervisor_image_is_warmed_on_every_node():
    docs = render("workspace.prewarm.enabled=true", RCLONE_PIN)
    daemonset = next(
        d
        for d in docs
        if d["kind"] == "DaemonSet" and "prewarm" in d["metadata"]["name"]
    )
    warm = {
        c["name"]: c for c in daemonset["spec"]["template"]["spec"]["initContainers"]
    }
    assert warm["pull-cloud-mount"]["image"] == (
        f"ghcr.io/superhuman-remote-worker/srw-cloud-mount@{RCLONE_DIGEST}"
    )
    assert warm["pull-cloud-mount"]["command"] == ["true"]


@pytest.mark.parametrize("named", ["opener", "rclone"])
def test_on_without_an_image_repository_fails_to_render(named):
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-n",
            "srw",
            "-f",
            str(ROOT / "helm/ci/test-values.yaml"),
            "--set",
            f"connectors.inPodPlane.{named}.image.repository=",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0
    assert (
        f"connectors.inPodPlane.enabled needs connectors.inPodPlane.{named}.image"
        in result.stderr
    )


@pytest.mark.parametrize(
    "setting",
    [
        "connectors.inPodPlane.opener.image.digest=sha256:nope",
        "connectors.inPodPlane.cacheSize=lots",
        "connectors.inPodPlane.drainSeconds=9999",
        "connectors.inPodPlane.maxMounts=0",
    ],
)
def test_the_schema_refuses_bad_settings(setting):
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "-n",
            "srw",
            "-f",
            str(ROOT / "helm/ci/test-values.yaml"),
            "--set",
            setting,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode != 0


def test_the_k3d_profile_uses_the_images_tilt_builds():
    values = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )["connectors"]["inPodPlane"]
    assert values["enabled"] is True
    assert (
        values["opener"]["image"]["repository"] == "srw-registry:5000/srw-fuse-opener"
    )
    assert (
        values["rclone"]["image"]["repository"] == "srw-registry:5000/srw-cloud-mount"
    )
    tiltfile = (ROOT / "Tiltfile").read_text()
    for image, target in (("srw-fuse-opener", "opener"), ("srw-cloud-mount", "rclone")):
        assert f"'{image}'" in tiltfile
        assert f"target='{target}'" in tiltfile


def test_every_installer_profile_renders_with_the_plane_on():
    """Installations get the plane at once: every shipped profile renders
    with it and grants create on Secrets."""
    for profile in sorted((ROOT / "helm/ci").glob("*values.yaml")):
        if profile.name == "invalid-values.yaml":
            continue
        result = subprocess.run(
            [
                "helm",
                "template",
                "srw",
                str(ROOT / "helm"),
                "-n",
                "srw",
                "-f",
                str(profile),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, (profile.name, result.stderr[-500:])
        docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
        env = orchestrator_env(docs)
        assert env.get("CONNECTOR_IN_POD_OPENER_IMAGE"), profile.name
        assert _secret_rules(docs), profile.name
