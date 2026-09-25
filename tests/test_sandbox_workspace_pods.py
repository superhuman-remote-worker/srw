"""Workspace pods take their shape from the resolved container profile."""

import logging
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.sandbox_workspace_settings import (
    SandboxSettings,
    sandbox_pod_profile,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner

JOB_ID = "11111111-2222-4333-8444-555555555555"
CUSTOM = "registry.example/team/workspace:1"


def build(provisioner, profile, *, pvc_name=None):
    return provisioner._build_pod_manifest(
        pod_name="workspace-111111112222",
        owner=WorkspaceOwner.job(JOB_ID),
        image=profile.image,
        cpu=profile.cpu,
        memory=profile.memory,
        cpu_limit=profile.cpu_limit,
        memory_limit=profile.memory_limit,
        pvc_name=pvc_name,
        profile=profile,
    )


def container(manifest):
    return manifest["spec"]["containers"][0]


def volume(manifest, name):
    return next(v for v in manifest["spec"]["volumes"] if v["name"] == name)


def test_empty_profile_builds_todays_pod():
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(SandboxSettings(), provisioner._image_policy())
    legacy = provisioner._build_pod_manifest(
        pod_name="workspace-111111112222",
        owner=WorkspaceOwner.job(JOB_ID),
        image=provisioner._workspace_image,
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
    )
    assert build(provisioner, profile) == legacy


def test_custom_image_pod_is_unprivileged_and_sized():
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(
        SandboxSettings(
            image=CUSTOM, pull_policy="Always", cpu=1, memory="3Gi", storage="15Gi"
        ),
        provisioner._image_policy(),
    )
    manifest = build(provisioner, profile)
    workspace = container(manifest)
    assert workspace["image"] == CUSTOM
    assert workspace["imagePullPolicy"] == "Always"
    assert workspace["resources"] == {
        "requests": {"cpu": "250m", "memory": "3Gi", "ephemeral-storage": "15Gi"},
        "limits": {"cpu": "1000m", "memory": "3Gi"},
    }
    assert volume(manifest, "workspace-data")["emptyDir"] == {"sizeLimit": "15Gi"}
    assert "privileged" not in workspace["securityContext"]
    assert "SYS_ADMIN" not in workspace["securityContext"]["capabilities"]["add"]
    assert manifest["spec"]["securityContext"]["seccompProfile"] == {
        "type": "RuntimeDefault"
    }
    assert all(v["name"] != "dev-fuse" for v in manifest["spec"]["volumes"])


def test_pvc_mode_uses_the_claim_without_an_ephemeral_request():
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(
        SandboxSettings(image=CUSTOM, storage="15Gi"), provisioner._image_policy()
    )
    manifest = build(provisioner, profile, pvc_name="pvc-workspace-111111112222")
    assert "ephemeral-storage" not in container(manifest)["resources"]["requests"]
    assert volume(manifest, "workspace-data")["persistentVolumeClaim"] == {
        "claimName": "pvc-workspace-111111112222"
    }


def test_resolved_default_pull_policy_reaches_the_pod():
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(
        SandboxSettings(image=CUSTOM, pull_policy="IfNotPresent"),
        provisioner._image_policy(),
    )
    assert container(build(provisioner, profile))["imagePullPolicy"] == "IfNotPresent"


def test_switch_gives_a_custom_image_the_fuse_profile():
    provisioner = ContainerProvisioner()
    policy = provisioner._image_policy()
    profile = sandbox_pod_profile(
        SandboxSettings(image=CUSTOM), replace(policy, custom_images_privileged=True)
    )
    manifest = build(provisioner, profile)
    assert container(manifest)["securityContext"].get("privileged") is (
        True if provisioner._fuse_privileged else None
    )
    assert any(v["name"] == "dev-fuse" for v in manifest["spec"]["volumes"]) is (
        provisioner._fuse_enabled
    )


def test_build_sha_label_only_for_the_installation_image():
    provisioner = ContainerProvisioner()
    provisioner._workspace_image = (
        "ghcr.io/superhuman-remote-worker/srw-workspace:sha-abc123"
    )
    default = sandbox_pod_profile(SandboxSettings(), provisioner._image_policy())
    custom = sandbox_pod_profile(
        SandboxSettings(image=CUSTOM), provisioner._image_policy()
    )
    assert build(provisioner, default)["metadata"]["labels"]["srw/build-sha"] == (
        "abc123"
    )
    assert "srw/build-sha" not in build(provisioner, custom)["metadata"]["labels"]


@pytest.mark.asyncio
async def test_reusing_a_smaller_pvc_is_logged_not_resized(caplog):
    provisioner = ContainerProvisioner()
    provisioner._k8s_available = True
    provisioner._core_api = SimpleNamespace(
        create_namespaced_persistent_volume_claim=None,
        read_namespaced_persistent_volume_claim=None,
    )
    conflict = Exception("exists")
    conflict.status = 409
    existing = SimpleNamespace(
        spec=SimpleNamespace(resources=SimpleNamespace(requests={"storage": "10Gi"}))
    )
    # `_create_pvc` makes its create call through `_bounded_kubernetes_mutation`,
    # which (by design, see its docstring) always dispatches through the class's
    # real `_bounded_kubernetes_call`, not `self`'s — so an instance override of
    # `_bounded_kubernetes_call` alone never sees the create call. Point both
    # names at the same mock so the shared side_effect list lines up with the
    # actual call order: create (raises the 409 conflict), then the reuse read.
    bounded_call = AsyncMock(side_effect=[conflict, existing])
    provisioner._bounded_kubernetes_call = bounded_call
    provisioner._bounded_kubernetes_mutation = bounded_call
    provisioner._require_stateless_pvc_identity = lambda *args, **kwargs: None
    with caplog.at_level(logging.WARNING):
        status = await provisioner._create_pvc(
            "pvc-ws-thread-666666667777",
            size="40Gi",
            expected_owner=WorkspaceOwner.session(
                "66666666-7777-4888-8999-aaaaaaaaaaaa"
            ),
        )
    assert status == "reused"
    assert "never resized" in caplog.text
