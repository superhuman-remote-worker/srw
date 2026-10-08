"""The in-pod plane's cloud mount sidecars (connector drivers D7, spike prototype).

The pod shape the k3d spike (scripts/k3d-in-pod-plane-spike.py) measured:
only the opener is privileged and only it propagates mounts; rclone and the
credential stay out of the workspace container; the workspace's view is
read-only and receives mounts; nothing opens a listener. Those hold only for
a workspace without privilege or SYS_ADMIN, so the default FUSE profile is
refused.
"""

from __future__ import annotations

import json

import pytest
from kubernetes.client import ApiClient

from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.in_pod_mount import (
    CLOUD_VOLUME,
    CREDENTIAL_VOLUME,
    OPENER_CONTAINER,
    RCLONE_CONTAINER,
    SOCKET_VOLUME,
    CloudMountSidecar,
    InPodPlaneImages,
    add_cloud_mount_sidecars,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner

JOB_ID = "11111111-2222-4333-8444-555555555555"
POD = "workspace-111111112222"
IMAGES = InPodPlaneImages(
    opener="registry.example/srw-fuse-opener:1@sha256:" + "1" * 64,
    rclone="registry.example/srw-cloud-mount:1@sha256:" + "2" * 64,
)
MOUNT = CloudMountSidecar(
    name="project", secret_name="ws-cloud-project", remote="cloud:Projects/x"
)


def build(
    provisioner: ContainerProvisioner, cloud_mount: CloudMountSidecar | None = None
) -> dict:
    return provisioner._build_pod_manifest(
        pod_name=POD,
        owner=WorkspaceOwner.job(JOB_ID),
        image=provisioner._workspace_image,
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
        cloud_mount=cloud_mount,
    )


def init(manifest: dict, name: str) -> dict:
    return next(c for c in manifest["spec"]["initContainers"] if c["name"] == name)


def mounts_of(container: dict) -> dict[str, dict]:
    return {m["name"]: m for m in container.get("volumeMounts", [])}


@pytest.fixture
def plane_on(monkeypatch):
    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", f" {IMAGES.opener} ")
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", IMAGES.rclone)
    # The only workspace profile the sidecars accept until D7 decides one.
    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", "false")
    return ContainerProvisioner()


@pytest.mark.parametrize(
    ("fuse_privileged", "why"), [("true", "privileged"), ("false", "SYS_ADMIN")]
)
def test_the_default_fuse_profile_is_refused(monkeypatch, fuse_privileged, why):
    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", IMAGES.opener)
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", IMAGES.rclone)
    monkeypatch.delenv("WORKSPACE_FUSE_ENABLED", raising=False)
    monkeypatch.setenv("WORKSPACE_FUSE_PRIVILEGED", fuse_privileged)
    provisioner = ContainerProvisioner()
    context = build(provisioner)["spec"]["containers"][0]["securityContext"]
    assert context.get("privileged", False) == (why == "privileged")
    assert "SYS_ADMIN" in context["capabilities"]["add"]
    with pytest.raises(ValueError, match="without privilege or SYS_ADMIN"):
        build(provisioner, MOUNT)


def test_a_workspace_adding_sys_admin_by_its_cap_name_is_refused(plane_on):
    manifest = build(plane_on)
    context = manifest["spec"]["containers"][0]["securityContext"]
    context["capabilities"]["add"] = [*context["capabilities"]["add"], "CAP_SYS_ADMIN"]
    with pytest.raises(ValueError, match="SYS_ADMIN"):
        add_cloud_mount_sidecars(manifest, MOUNT, IMAGES)


def test_the_plane_is_off_without_both_images(monkeypatch):
    monkeypatch.delenv("CONNECTOR_IN_POD_OPENER_IMAGE", raising=False)
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", IMAGES.rclone)
    assert InPodPlaneImages.from_env() is None
    provisioner = ContainerProvisioner()
    with pytest.raises(ValueError, match="inPodPlane"):
        build(provisioner, MOUNT)


@pytest.mark.parametrize("fuse", ["true", "false"])
def test_without_a_cloud_mount_the_pod_is_todays(monkeypatch, fuse):
    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", fuse)
    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", IMAGES.opener)
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", IMAGES.rclone)
    on = ContainerProvisioner()
    monkeypatch.delenv("CONNECTOR_IN_POD_OPENER_IMAGE")
    monkeypatch.delenv("CONNECTOR_IN_POD_RCLONE_IMAGE")
    assert build(on) == build(ContainerProvisioner())
    assert "initContainers" not in build(on)["spec"]


def test_the_opener_starts_first_and_alone_is_privileged(plane_on):
    manifest = build(plane_on, MOUNT)
    spec = manifest["spec"]
    assert [c["name"] for c in spec["initContainers"]] == [
        OPENER_CONTAINER,
        RCLONE_CONTAINER,
    ]
    assert all(c["restartPolicy"] == "Always" for c in spec["initContainers"])
    opener = init(manifest, OPENER_CONTAINER)
    assert opener["image"] == IMAGES.opener
    assert opener["securityContext"] == {"privileged": True}
    assert opener["args"] == [
        "serve",
        "--socket",
        "/run/srw-fuse/opener.sock",
        "--target",
        "/srw/cloud/project",
        "--client-uid",
        "65534",
        "--read-only",
    ]
    # The only Bidirectional mount in the pod, on the memory emptyDir.
    propagating = [
        (c["name"], m["name"])
        for c in [*spec["initContainers"], *spec["containers"]]
        for m in c.get("volumeMounts", [])
        if m.get("mountPropagation") == "Bidirectional"
    ]
    assert propagating == [(OPENER_CONTAINER, CLOUD_VOLUME)]
    cloud = next(v for v in spec["volumes"] if v["name"] == CLOUD_VOLUME)
    assert cloud["emptyDir"]["medium"] == "Memory"


def test_rclone_runs_unprivileged_and_alone_holds_the_credential(plane_on):
    manifest = build(plane_on, MOUNT)
    rclone = init(manifest, RCLONE_CONTAINER)
    assert rclone["image"] == IMAGES.rclone
    assert rclone["securityContext"] == {
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "runAsNonRoot": True,
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert rclone["command"] == ["rclone"]
    assert rclone["args"][:3] == ["mount2", "cloud:Projects/x", "/srw/cloud/project"]
    assert "--read-only" in rclone["args"]
    assert mounts_of(rclone)[CLOUD_VOLUME]["mountPropagation"] == "HostToContainer"
    # rclone reaches the opener's socket but can place nothing beside it.
    assert mounts_of(rclone)[SOCKET_VOLUME]["readOnly"] is True
    assert "readOnly" not in mounts_of(init(manifest, OPENER_CONTAINER))[SOCKET_VOLUME]
    assert all(
        SOCKET_VOLUME not in mounts_of(c) for c in manifest["spec"]["containers"]
    )
    holders = [
        c["name"]
        for c in [*manifest["spec"]["initContainers"], *manifest["spec"]["containers"]]
        if CREDENTIAL_VOLUME in mounts_of(c)
    ]
    assert holders == [RCLONE_CONTAINER]
    credential = next(
        v for v in manifest["spec"]["volumes"] if v["name"] == CREDENTIAL_VOLUME
    )
    assert credential["secret"]["secretName"] == "ws-cloud-project"
    assert "fsGroup" not in manifest["spec"].get("securityContext", {})


def test_no_sidecar_opens_a_listener(plane_on):
    manifest = build(plane_on, MOUNT)
    for container in manifest["spec"]["initContainers"]:
        assert "ports" not in container
        assert not any(arg.startswith("--rc") for arg in container.get("args", []))
    assert manifest["spec"].get("shareProcessNamespace") is not True


def test_the_workspace_sees_the_mount_read_only_and_nothing_else(plane_on):
    manifest = build(plane_on, MOUNT)
    before = mounts_of(build(ContainerProvisioner())["spec"]["containers"][0])
    workspace = manifest["spec"]["containers"][0]
    added = {k: v for k, v in mounts_of(workspace).items() if k not in before}
    assert added == {
        CLOUD_VOLUME: {
            "name": CLOUD_VOLUME,
            "mountPath": "/cloud",
            "readOnly": True,
            "mountPropagation": "HostToContainer",
        }
    }
    # The workspace container's own profile is unchanged by the plane.
    assert (
        workspace["securityContext"]
        == build(ContainerProvisioner())["spec"]["containers"][0]["securityContext"]
    )


def test_a_read_write_mount_drops_both_read_only_flags(plane_on):
    manifest = build(
        plane_on,
        CloudMountSidecar(
            name="outbox", secret_name="ws-outbox", remote="cloud:", read_only=False
        ),
    )
    assert "--read-only" not in init(manifest, OPENER_CONTAINER)["args"]
    assert "--read-only" not in init(manifest, RCLONE_CONTAINER)["args"]
    # The workspace's view of the emptyDir stays read-only either way.
    assert (
        mounts_of(manifest["spec"]["containers"][0])[CLOUD_VOLUME]["readOnly"] is True
    )


def test_the_storage_authority_check_still_accepts_the_pod(plane_on):
    manifest = build(plane_on, MOUNT)

    class Response:
        data = json.dumps(manifest)

    pod = ApiClient().deserialize(Response(), "V1Pod")
    assert (
        plane_on._require_stateless_pod_storage_binding(
            pod,
            owner=WorkspaceOwner.job(JOB_ID),
            expected_pvc_name=None,
            expected_seed_configmap=None,
        )
        is None
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"name": "../etc"},
        {"name": "Project"},
        {"secret_name": "a b"},
        {"remote": "no-colon"},
        {"remote": "cloud:\nextra"},
    ],
)
def test_malformed_mounts_are_refused(kwargs):
    fields = {"name": "project", "secret_name": "s", "remote": "cloud:"} | kwargs
    with pytest.raises(ValueError):
        CloudMountSidecar(**fields)


def test_a_shared_pid_namespace_or_a_second_pair_is_refused(plane_on):
    manifest = build(plane_on)
    manifest["spec"]["shareProcessNamespace"] = True
    with pytest.raises(ValueError, match="PID namespace"):
        add_cloud_mount_sidecars(manifest, MOUNT, IMAGES)
    manifest = build(plane_on, MOUNT)
    with pytest.raises(ValueError, match="already"):
        add_cloud_mount_sidecars(manifest, MOUNT, IMAGES)
