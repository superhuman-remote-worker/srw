"""The in-pod plane's cloud mount sidecars (connector drivers D7).

The Pod shape the k3d spike (scripts/k3d-in-pod-plane-spike.py) measured,
grown to several mounts: only the opener is privileged and only it
propagates mounts; the supervisor, rclone and the credential stay out of the
workspace container; the workspace's view is read-only and receives mounts;
nothing opens a listener; no probe the kubelet waits on depends on a remote.
A privileged workspace is refused unless the Pod is protected (its capture
overlay still needs FUSE). The provisioner's half: the plan joins the digest
and the fingerprint only when present, a sidecar Pod's workspace has no FUSE,
and the plan ConfigMap and credential Secret are created owned by the Pod.
"""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from kubernetes.client import ApiClient

from orchestrator.services.cloud_mount_plan import (
    CloudMountPlan,
    SidecarMount,
    rclone_reveal,
)
from orchestrator.services.cloud_mount_sidecar import (
    PLAN_ANNOTATION,
    PLAN_CONTEXT_KEY,
    recorded_plan_from_annotations,
)
from orchestrator.services.container_provisioner import ContainerProvisioner
from orchestrator.services.in_pod_mount import (
    CACHE_VOLUME,
    CLOUD_VOLUME,
    CONTROL_VOLUME,
    CREDENTIAL_VOLUME,
    OPENER_CONTAINER,
    PLAN_VOLUME,
    RCLONE_CONTAINER,
    SOCKET_VOLUME,
    STATUS_VOLUME,
    InPodPlaneSettings,
    SidecarSpec,
    add_cloud_mount_sidecars,
    objects_name_for,
    objects_name_from_pod,
    workspace_can_mount,
)
from orchestrator.services.sandbox_workspace_settings import SandboxPodProfile
from orchestrator.services.workspace_lifecycle import WorkspaceOwner

THREAD_ID = "11111111-2222-4333-8444-555555555555"
RESERVATION = "66666666-6666-4666-8666-666666666666"
POD_UID = "77777777-7777-4777-8777-777777777777"
OPENER = "registry.example/srw-fuse-opener:1@sha256:" + "1" * 64
RCLONE = "registry.example/srw-cloud-mount:1@sha256:" + "2" * 64
SETTINGS = InPodPlaneSettings(opener_image=OPENER, rclone_image=RCLONE)
PASSWORD = "agent-service-password"


def _mount(index: int, name: str, access: str = "read_write") -> SidecarMount:
    return SidecarMount(
        index=index,
        name=name,
        mount_id=f"row-{name}",
        mount_kind="project",
        source_ref=f"project-{name}",
        backend="nextcloud",
        access=access,
        source_type="webdav",
        source_config=(
            ("url", f"http://srw-nextcloud/remote.php/dav/{name}/"),
            ("vendor", "nextcloud"),
            ("user", "agent-service"),
        ),
        root="",
        flags=("--vfs-cache-mode", "full"),
    )


PLAN = CloudMountPlan(
    mounts=(_mount(0, "project"), _mount(1, "reference", "read_only")),
    excluded=(),
    drain_seconds=60,
    cache_size="10Gi",
    passwords={0: PASSWORD, 1: PASSWORD},
)
EMPTY_PLAN = CloudMountPlan(mounts=(), excluded=(), drain_seconds=60, cache_size="10Gi")


def build(
    provisioner: ContainerProvisioner, plan: CloudMountPlan | None = None
) -> dict:
    owner = WorkspaceOwner.session(THREAD_ID)
    profile = None
    if plan is not None:
        base = SandboxPodProfile(
            image=provisioner._workspace_image,
            cpu="500m",
            memory="1Gi",
            cpu_limit="2000m",
            memory_limit="4Gi",
            pull_policy=None,
            storage=None,
            fuse_enabled=True,
            fuse_privileged=True,
            templated=False,
        )
        profile = provisioner._profile_for_cloud_plan(base, plan)
    return provisioner._build_pod_manifest(
        pod_name=owner.pod_name,
        owner=owner,
        image=provisioner._workspace_image,
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
        creation_reservation_id=RESERVATION,
        profile=profile,
        cloud_plan=plan,
    )


def init(manifest: dict, name: str) -> dict:
    return next(c for c in manifest["spec"]["initContainers"] if c["name"] == name)


def mounts_of(container: dict) -> dict[str, dict]:
    return {m["name"]: m for m in container.get("volumeMounts", [])}


def volume(manifest: dict, name: str) -> dict:
    return next(v for v in manifest["spec"]["volumes"] if v["name"] == name)


@pytest.fixture
def plane_on(monkeypatch):
    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", f" {OPENER} ")
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", RCLONE)
    return ContainerProvisioner()


# --------------------------------------------------------------------------- #
# Settings
# --------------------------------------------------------------------------- #


def test_the_plane_is_off_without_both_images(monkeypatch):
    monkeypatch.delenv("CONNECTOR_IN_POD_OPENER_IMAGE", raising=False)
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", RCLONE)
    assert InPodPlaneSettings.from_env() is None
    with pytest.raises(ValueError, match="inPodPlane"):
        build(ContainerProvisioner(), PLAN)


def test_the_plane_reads_its_bounded_settings(monkeypatch):
    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", OPENER)
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", RCLONE)
    monkeypatch.setenv("CONNECTOR_IN_POD_CACHE_SIZE", "20Gi")
    monkeypatch.setenv("CONNECTOR_IN_POD_DRAIN_SECONDS", "9999")
    monkeypatch.setenv("CONNECTOR_IN_POD_MAX_MOUNTS", "0")
    settings = InPodPlaneSettings.from_env()
    assert (settings.cache_size, settings.drain_seconds, settings.max_mounts) == (
        "20Gi",
        600,
        1,
    )
    assert settings.images_pinned
    monkeypatch.setenv("CONNECTOR_IN_POD_CACHE_SIZE", "lots")
    monkeypatch.setenv("CONNECTOR_IN_POD_DRAIN_SECONDS", "x")
    settings = InPodPlaneSettings.from_env()
    assert (settings.cache_size, settings.drain_seconds) == ("10Gi", 60)


# --------------------------------------------------------------------------- #
# The Pod
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("fuse", ["true", "false"])
def test_without_a_plan_the_pod_is_todays(monkeypatch, fuse):
    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", fuse)
    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", OPENER)
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", RCLONE)
    on = ContainerProvisioner()
    monkeypatch.delenv("CONNECTOR_IN_POD_OPENER_IMAGE")
    monkeypatch.delenv("CONNECTOR_IN_POD_RCLONE_IMAGE")
    assert build(on) == build(ContainerProvisioner())
    assert "initContainers" not in build(on)["spec"]
    assert PLAN_ANNOTATION not in build(on)["metadata"]["annotations"]


def test_an_empty_plan_has_no_sidecars_and_no_fuse(plane_on):
    manifest = build(plane_on, EMPTY_PLAN)
    assert "initContainers" not in manifest["spec"]
    assert recorded_plan_from_annotations(manifest["metadata"]["annotations"]) == (
        EMPTY_PLAN.recorded()
    )
    context = manifest["spec"]["containers"][0]["securityContext"]
    assert not context.get("privileged")
    assert "SYS_ADMIN" not in context["capabilities"].get("add", [])
    assert (
        manifest["spec"]["securityContext"]["seccompProfile"]["type"]
        == "RuntimeDefault"
    )
    assert all(v["name"] != "fuse-device" for v in manifest["spec"]["volumes"])


def test_the_opener_starts_first_alone_is_privileged_and_owns_every_target(plane_on):
    manifest = build(plane_on, PLAN)
    spec = manifest["spec"]
    assert [c["name"] for c in spec["initContainers"]] == [
        OPENER_CONTAINER,
        RCLONE_CONTAINER,
    ]
    assert all(c["restartPolicy"] == "Always" for c in spec["initContainers"])
    opener = init(manifest, OPENER_CONTAINER)
    assert opener["image"] == OPENER
    assert opener["securityContext"] == {"privileged": True}
    assert opener["args"] == [
        "serve",
        "--socket",
        "/run/srw-fuse/opener.sock",
        "--client-uid",
        "65534",
        "--target",
        "/srw/cloud/project",
        "--target",
        "/srw/cloud/reference:ro",
    ]
    # Its probe is local: its own socket. Nothing remote is waited for.
    assert opener["startupProbe"]["exec"]["command"][1] == "ping"
    propagating = [
        (c["name"], m["name"])
        for c in [*spec["initContainers"], *spec["containers"]]
        for m in c.get("volumeMounts", [])
        if m.get("mountPropagation") == "Bidirectional"
    ]
    assert propagating == [(OPENER_CONTAINER, CLOUD_VOLUME)]
    assert volume(manifest, CLOUD_VOLUME)["emptyDir"]["medium"] == "Memory"


def test_the_supervisor_runs_unprivileged_holds_the_credential_and_has_no_probe(
    plane_on,
):
    manifest = build(plane_on, PLAN)
    supervisor = init(manifest, RCLONE_CONTAINER)
    assert supervisor["image"] == RCLONE
    assert supervisor["securityContext"] == {
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "runAsNonRoot": True,
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert supervisor["command"] == ["srw-cloud-mount"]
    # A mount that does not come up must never hold the workspace back.
    assert "startupProbe" not in supervisor and "readinessProbe" not in supervisor
    assert supervisor["env"] == [{"name": "HOME", "value": "/tmp"}]
    assert mounts_of(supervisor)[CLOUD_VOLUME]["mountPropagation"] == "HostToContainer"
    assert mounts_of(supervisor)[SOCKET_VOLUME]["readOnly"] is True
    assert mounts_of(supervisor)[CONTROL_VOLUME]["readOnly"] is True
    assert "readOnly" not in mounts_of(supervisor)[STATUS_VOLUME]
    every = [*manifest["spec"]["initContainers"], *manifest["spec"]["containers"]]
    for private in (CREDENTIAL_VOLUME, PLAN_VOLUME, CACHE_VOLUME, SOCKET_VOLUME):
        holders = {c["name"] for c in every if private in mounts_of(c)}
        assert "workspace" not in holders, private
    assert {c["name"] for c in every if CREDENTIAL_VOLUME in mounts_of(c)} == {
        RCLONE_CONTAINER
    }
    credential = volume(manifest, CREDENTIAL_VOLUME)["secret"]
    assert (
        credential["secretName"] == volume(manifest, PLAN_VOLUME)["configMap"]["name"]
    )
    assert credential["secretName"] == objects_name_for(
        WorkspaceOwner.session(THREAD_ID).pod_name, RESERVATION
    )
    assert "optional" not in credential
    assert volume(manifest, CACHE_VOLUME)["emptyDir"] == {"sizeLimit": "10Gi"}
    assert "fsGroup" not in manifest["spec"].get("securityContext", {})


def test_no_sidecar_opens_a_listener(plane_on):
    manifest = build(plane_on, PLAN)
    for container in manifest["spec"]["initContainers"]:
        assert "ports" not in container
        assert not any(arg.startswith("--rc") for arg in container.get("args", []))
    assert manifest["spec"].get("shareProcessNamespace") is not True


def test_the_workspace_sees_mounts_and_status_read_only_and_may_only_ask(plane_on):
    manifest = build(plane_on, PLAN)
    before = mounts_of(build(ContainerProvisioner())["spec"]["containers"][0])
    workspace = manifest["spec"]["containers"][0]
    added = {k: v for k, v in mounts_of(workspace).items() if k not in before}
    assert added == {
        CLOUD_VOLUME: {
            "name": CLOUD_VOLUME,
            "mountPath": "/cloud",
            "readOnly": True,
            "mountPropagation": "HostToContainer",
        },
        STATUS_VOLUME: {
            "name": STATUS_VOLUME,
            "mountPath": "/srw/cloud-status",
            "readOnly": True,
        },
        CONTROL_VOLUME: {"name": CONTROL_VOLUME, "mountPath": "/srw/cloud-control"},
    }
    context = workspace["securityContext"]
    assert not context.get("privileged")
    assert "SYS_ADMIN" not in context["capabilities"].get("add", [])


def test_a_workspace_that_could_mount_is_refused_unless_protected(plane_on):
    manifest = build(plane_on)
    workspace = manifest["spec"]["containers"][0]
    workspace["securityContext"]["capabilities"]["add"] = [
        *workspace["securityContext"]["capabilities"]["add"],
        "CAP_SYS_ADMIN",
    ]
    spec = PLAN.sidecar_spec("ws-cloud-x")
    with pytest.raises(ValueError, match="SYS_ADMIN"):
        add_cloud_mount_sidecars(json.loads(json.dumps(manifest)), spec, SETTINGS)
    protected = SidecarSpec(
        targets=(("/srw/cloud/lower", True),),
        objects_name="ws-cloud-x",
        dirs=("/srw/cloud/merged",),
        protected=True,
    )
    add_cloud_mount_sidecars(manifest, protected, SETTINGS)
    workspace_mounts = mounts_of(manifest["spec"]["containers"][0])
    # The overlay mounts on the opener's directory itself, which agent-host
    # owns: a nested volume would be a mountpoint the overlay's scripts read
    # as an overlay already up, and a non-root fusermount3 needs write access.
    assert workspace_mounts[CLOUD_VOLUME] == {
        "name": CLOUD_VOLUME,
        "mountPath": "/cloud",
        "mountPropagation": "HostToContainer",
    }
    assert [v["name"] for v in manifest["spec"]["volumes"]].count(CLOUD_VOLUME) == 1
    assert not any(
        m["mountPath"].startswith("/cloud/")
        for m in manifest["spec"]["containers"][0]["volumeMounts"]
    )
    assert init(manifest, OPENER_CONTAINER)["args"][-4:] == [
        "--dir",
        "/srw/cloud/merged",
        "--dir-uid",
        "1000",
    ]


def test_a_second_directory_a_shared_pid_namespace_or_a_second_pair_is_refused(
    plane_on,
):
    manifest = build(plane_on)
    two = SidecarSpec(
        targets=(("/srw/cloud/a", False),),
        objects_name="x",
        dirs=("/srw/cloud/b", "/srw/cloud/c"),
        protected=True,
    )
    with pytest.raises(ValueError, match="at most one"):
        add_cloud_mount_sidecars(manifest, two, SETTINGS)
    unprotected = SidecarSpec(
        targets=(("/srw/cloud/a", False),), objects_name="x", dirs=("/srw/cloud/b",)
    )
    with pytest.raises(ValueError, match="only a protected"):
        add_cloud_mount_sidecars(manifest, unprotected, SETTINGS)
    manifest["spec"]["shareProcessNamespace"] = True
    with pytest.raises(ValueError, match="PID namespace"):
        add_cloud_mount_sidecars(manifest, PLAN.sidecar_spec("x"), SETTINGS)
    manifest = build(plane_on, PLAN)
    with pytest.raises(ValueError, match="already"):
        add_cloud_mount_sidecars(manifest, PLAN.sidecar_spec("x"), SETTINGS)


def test_the_storage_authority_check_still_accepts_the_pod(plane_on):
    manifest = build(plane_on, PLAN)

    class Response:
        data = json.dumps(manifest)

    pod = ApiClient().deserialize(Response(), "V1Pod")
    assert (
        plane_on._require_stateless_pod_storage_binding(
            pod,
            owner=WorkspaceOwner.session(THREAD_ID),
            expected_pvc_name=None,
            expected_seed_configmap=None,
        )
        is None
    )
    assert (
        objects_name_from_pod(pod)
        == volume(manifest, CREDENTIAL_VOLUME)["secret"]["secretName"]
    )
    assert objects_name_from_pod(manifest) == objects_name_from_pod(pod)


def test_each_creation_attempt_names_its_own_objects():
    assert objects_name_for("ws-a", "attempt-1") != objects_name_for(
        "ws-a", "attempt-2"
    )
    assert objects_name_for("ws-a", "attempt-1") == objects_name_for(
        "ws-a", "attempt-1"
    )
    assert objects_name_for("ws-a", None) == "ws-a-cloud"


# --------------------------------------------------------------------------- #
# The provisioner's half
# --------------------------------------------------------------------------- #


def _profile(fuse: bool = True) -> SandboxPodProfile:
    return SandboxPodProfile(
        image="img",
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
        pull_policy=None,
        storage=None,
        fuse_enabled=fuse,
        fuse_privileged=fuse,
        templated=False,
    )


def test_only_a_sidecar_pod_loses_fuse_and_a_protected_one_keeps_it():
    profile = _profile()
    assert ContainerProvisioner._profile_for_cloud_plan(profile, None) is profile
    plain = ContainerProvisioner._profile_for_cloud_plan(profile, PLAN)
    assert (plain.fuse_enabled, plain.fuse_privileged) == (False, False)
    protected = CloudMountPlan(
        mounts=(), excluded=(), drain_seconds=60, cache_size="10Gi", protected=True
    )
    assert ContainerProvisioner._profile_for_cloud_plan(profile, protected) is profile


def test_a_protected_pod_keeps_fuse_for_its_overlay_on_the_sidecars_lower(plane_on):
    lower = SidecarMount(
        index=0,
        name="lower",
        mount_id=f"protected-{THREAD_ID}",
        mount_kind="protected_lower",
        source_ref=None,
        backend="nextcloud",
        access="read_only",
        source_type="webdav",
        source_config=(("url", "https://nc/remote.php/dav/files/srw-reader-u/"),),
        root="",
        flags=(),
    )
    plan = CloudMountPlan(
        mounts=(lower,),
        excluded=(),
        drain_seconds=60,
        cache_size="10Gi",
        passwords={0: "reader-secret"},
        protected=True,
        overlay={"lower": "/cloud/lower", "merged": "/cloud/merged"},
    )
    manifest = build(ContainerProvisioner(), plan)
    workspace = manifest["spec"]["containers"][0]
    # The overlay (fuse-overlayfs) still runs in the workspace.
    assert workspace_can_mount(workspace)
    opener = init(manifest, OPENER_CONTAINER)
    assert opener["args"][5:] == [
        "--target",
        "/srw/cloud/lower:ro",
        "--dir",
        "/srw/cloud/merged",
        "--dir-uid",
        "1000",
    ]
    view = mounts_of(workspace)[CLOUD_VOLUME]
    assert "readOnly" not in view and view["mountPropagation"] == "HostToContainer"
    assert "reader-secret" not in json.dumps(manifest)


def _fingerprint(provisioner: ContainerProvisioner, plan=None) -> str:
    owner = WorkspaceOwner.session(THREAD_ID)
    return provisioner._pinned_workspace_provision_fingerprint(
        owner=owner,
        pod_name=owner.pod_name,
        pvc_name=None,
        seed_configmap_name=None,
        service_name=None,
        network_tier="default",
        workspace_image="img",
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
        seed_files={},
        seed_extensions={},
        seed_needs_state=False,
        profile=_profile(),
        cloud_plan=plan,
    )


def test_the_plan_joins_the_fingerprint_and_digest_only_when_present(plane_on):
    off = ContainerProvisioner()
    assert _fingerprint(plane_on) == _fingerprint(off)
    assert _fingerprint(plane_on, PLAN) != _fingerprint(plane_on)
    assert _fingerprint(plane_on, PLAN) != _fingerprint(plane_on, EMPTY_PLAN)
    assert plane_on._cloud_plan_digest_input(None) is None
    assert plane_on._cloud_plan_digest_input(PLAN) == PLAN.digest_input(
        plane_on._in_pod_plane
    )


@pytest.mark.asyncio
async def test_the_creation_plan_digest_is_unchanged_without_a_plan(plane_on):
    owner = WorkspaceOwner.session(THREAD_ID)
    plane_on._resolve_ide_seed_files = AsyncMock(return_value={})
    plane_on._resolve_ide_extensions = AsyncMock(return_value={})
    plane_on._resolve_ide_needs_state = AsyncMock(return_value=False)
    plane_on._resolve_network_tier = AsyncMock(return_value="default")
    without = await plane_on._workspace_creation_plan(
        owner, profile=_profile(), stateless_creation_generation=None
    )
    assert "cloud_mounts" not in without
    with_plan = await plane_on._workspace_creation_plan(
        owner, profile=_profile(), stateless_creation_generation=None, cloud_plan=PLAN
    )
    assert with_plan["cloud_mounts"] == PLAN.digest_input(plane_on._in_pod_plane)
    assert with_plan["digest"] != without["digest"]


def _pod(manifest: dict, uid: str = POD_UID):
    class Response:
        data = json.dumps(
            {**manifest, "metadata": {**manifest["metadata"], "uid": uid}}
        )

    return ApiClient().deserialize(Response(), "V1Pod")


class _CoreApi:
    def __init__(self, fail_with: int | None = None) -> None:
        self.created: list[tuple[str, dict]] = []
        self.fail_with = fail_with

    def _create(self, kind, namespace, body):
        if self.fail_with is not None:
            error = RuntimeError("apiserver says no")
            error.status = self.fail_with
            raise error
        self.created.append((kind, body))
        return body

    def create_namespaced_config_map(self, namespace, body):
        return self._create("ConfigMap", namespace, body)

    def create_namespaced_secret(self, namespace, body):
        return self._create("Secret", namespace, body)


def _with_api(provisioner: ContainerProvisioner, api: _CoreApi) -> ContainerProvisioner:
    provisioner._core_api = api

    async def mutation(call, **kwargs):
        return call(**kwargs)

    provisioner._bounded_kubernetes_mutation = mutation
    return provisioner


@pytest.mark.asyncio
async def test_the_plan_and_credential_objects_are_owned_by_the_pod_from_birth(
    plane_on, caplog
):
    api = _CoreApi()
    provisioner = _with_api(plane_on, api)
    pod = _pod(build(provisioner, PLAN))
    owner = WorkspaceOwner.session(THREAD_ID)
    with caplog.at_level(logging.DEBUG):
        assert await provisioner._ensure_cloud_mount_objects(pod, PLAN, owner=owner)
    (cm_kind, configmap), (secret_kind, secret) = api.created
    assert (cm_kind, secret_kind) == ("ConfigMap", "Secret")
    for body in (configmap, secret):
        assert body["metadata"]["name"] == objects_name_from_pod(pod)
        (reference,) = body["metadata"]["ownerReferences"]
        assert reference["kind"] == "Pod" and reference["uid"] == POD_UID
        assert reference["blockOwnerDeletion"] is False
    assert json.loads(configmap["data"]["plan.json"]) == PLAN.supervisor_plan()
    assert secret["immutable"] is True and secret["type"] == "Opaque"
    config = secret["stringData"]["rclone.conf"]
    assert PASSWORD not in config
    passes = [
        line.split(" = ", 1)[1]
        for line in config.splitlines()
        if line.startswith("pass = ")
    ]
    assert [rclone_reveal(p) for p in passes] == [PASSWORD, PASSWORD]
    assert PASSWORD not in caplog.text and passes[0] not in caplog.text


@pytest.mark.asyncio
async def test_an_earlier_create_of_the_same_attempt_is_fine_any_other_error_is_not(
    plane_on, caplog
):
    owner = WorkspaceOwner.session(THREAD_ID)
    pod = _pod(build(plane_on, PLAN))
    assert await _with_api(
        plane_on, _CoreApi(fail_with=409)
    )._ensure_cloud_mount_objects(pod, PLAN, owner=owner)
    with caplog.at_level(logging.ERROR):
        assert not await _with_api(
            plane_on, _CoreApi(fail_with=403)
        )._ensure_cloud_mount_objects(pod, PLAN, owner=owner)
    assert "Secret" in caplog.text or "ConfigMap" in caplog.text
    assert "apiserver says no" not in caplog.text


@pytest.mark.asyncio
async def test_no_objects_without_mounts_or_for_a_pod_of_another_plan(plane_on):
    api = _CoreApi()
    provisioner = _with_api(plane_on, api)
    owner = WorkspaceOwner.session(THREAD_ID)
    assert await provisioner._ensure_cloud_mount_objects(
        _pod(build(provisioner, EMPTY_PLAN)), EMPTY_PLAN, owner=owner
    )
    other = CloudMountPlan(
        mounts=(_mount(0, "other"),),
        excluded=(),
        drain_seconds=60,
        cache_size="10Gi",
        passwords={0: PASSWORD},
    )
    assert await provisioner._ensure_cloud_mount_objects(
        _pod(build(provisioner, other)), PLAN, owner=owner
    )
    assert api.created == []


@pytest.mark.asyncio
async def test_the_pods_plan_is_recorded_tied_to_its_uid(plane_on):
    store = SimpleNamespace(
        merge_thread_workspace_context=AsyncMock(return_value=True),
    )
    statuses: list = []

    class _Store:
        merge_thread_workspace_context = store.merge_thread_workspace_context

        async def set_thread_cloud_mount_status(self, thread_id, status):
            statuses.append(status)
            return True

    plane_on._db = _Store()
    owner = WorkspaceOwner.session(THREAD_ID)
    assert await plane_on._publish_cloud_mount_plan(
        owner, _pod(build(plane_on, PLAN)), POD_UID
    )
    (thread_id, update), _ = store.merge_thread_workspace_context.await_args
    assert thread_id == THREAD_ID
    assert update[PLAN_CONTEXT_KEY] == {
        **PLAN.recorded(),
        "runtime_incarnation": POD_UID,
    }
    (status,) = statuses
    assert status["fingerprint"] == PLAN.recorded()["fingerprint"]
    assert {name: entry["state"] for name, entry in status["mounts"].items()} == {
        "project": "pending",
        "reference": "pending",
    }
    # An in-workspace Pod clears an earlier Pod's plan and state.
    store.merge_thread_workspace_context.reset_mock()
    statuses.clear()
    assert await plane_on._publish_cloud_mount_plan(
        owner, _pod(build(plane_on)), POD_UID
    )
    assert store.merge_thread_workspace_context.await_args[0][1] == {
        PLAN_CONTEXT_KEY: None
    }
    assert statuses == [None]


@pytest.mark.asyncio
async def test_with_the_plane_off_a_pod_writes_nothing_new():
    provisioner = ContainerProvisioner()
    provisioner._db = SimpleNamespace(merge_thread_workspace_context=AsyncMock())
    assert await provisioner._publish_cloud_mount_plan(
        WorkspaceOwner.session(THREAD_ID), _pod(build(provisioner)), POD_UID
    )
    provisioner._db.merge_thread_workspace_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_planning_is_for_sessions_and_its_failure_keeps_the_old_path(
    plane_on, caplog
):
    plane_on._cloud_mount_planner = AsyncMock(return_value=PLAN)
    assert await plane_on._cloud_mount_plan_for(WorkspaceOwner.job(THREAD_ID)) is None
    assert (
        await plane_on._cloud_mount_plan_for(WorkspaceOwner.session(THREAD_ID)) is PLAN
    )
    plane_on._cloud_mount_planner = AsyncMock(side_effect=RuntimeError("boom"))
    with caplog.at_level(logging.ERROR):
        assert (
            await plane_on._cloud_mount_plan_for(WorkspaceOwner.session(THREAD_ID))
            is None
        )
    assert "in-workspace mount path" in caplog.text
