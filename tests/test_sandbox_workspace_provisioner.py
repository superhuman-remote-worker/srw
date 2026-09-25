"""ContainerProvisioner resolves frozen container settings for every creation."""

import hashlib
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services import container_provisioner as provisioner_module
from orchestrator.services.container_provisioner import (
    ContainerProvisioner,
    _canonical_manifest_digest,
)
from orchestrator.services.sandbox_workspace_settings import (
    SandboxSettings,
    sandbox_pod_profile,
)
from orchestrator.services.workspace_lifecycle import WorkspaceOwner
from tests.test_container_provisioner import (
    _TEST_POD_UID,
    _TEST_RESOURCE_UID,
    _PinnedWorkspaceIntentDB,
    _pod_from_manifest,
    _pvc_from_manifest,
    _service_from_manifest,
)

JOB_ID = "11111111-2222-4333-8444-555555555555"
THREAD_ID = "66666666-7777-4888-8999-aaaaaaaaaaaa"
CUSTOM = "registry.example/team/workspace:1"


def stub_plan_inputs(monkeypatch, provisioner):
    for name, value in (
        ("_resolve_ide_seed_files", {}),
        ("_resolve_ide_extensions", {}),
        ("_resolve_ide_needs_state", False),
        ("_resolve_network_tier", "internet-only"),
    ):
        monkeypatch.setattr(provisioner, name, AsyncMock(return_value=value))


@pytest.mark.asyncio
@pytest.mark.parametrize("fuse", ["true", "false"])
@pytest.mark.parametrize("pvc_enabled", [False, True])
async def test_empty_template_plan_is_byte_identical_to_the_legacy_plan(
    monkeypatch, pvc_enabled, fuse
):
    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", fuse)
    provisioner = ContainerProvisioner()
    provisioner._pvc_enabled = pvc_enabled
    stub_plan_inputs(monkeypatch, provisioner)
    profile = sandbox_pod_profile(SandboxSettings(), provisioner._image_policy())
    plan = await provisioner._workspace_creation_plan(
        WorkspaceOwner.job(JOB_ID), profile=profile, stateless_creation_generation=None
    )
    legacy = {
        "version": 1,
        "scope": "workspace_container",
        "owner_kind": "job",
        "image": provisioner._workspace_image,
        "cpu": "500m",
        "memory": "1Gi",
        "cpu_limit": "2000m",
        "memory_limit": "4Gi",
        "network_tier": "internet-only",
        "pvc": {
            "enabled": provisioner._pvc_enabled,
            "size": provisioner._pvc_size if provisioner._pvc_enabled else None,
            "storage_class": (
                provisioner._storage_class if provisioner._pvc_enabled else None
            ),
        },
        "seed_files": {},
        "seed_extensions": {},
        "seed_needs_state": False,
        "stateless_generation": None,
        "runtime_policy": {
            "ssh_secret_name": provisioner._ssh_secret_name,
            "fuse_enabled": provisioner._fuse_enabled,
            "fuse_privileged": provisioner._fuse_privileged,
        },
    }
    assert plan["digest"] == _canonical_manifest_digest(legacy)


@pytest.mark.asyncio
async def test_template_settings_reach_the_plan(monkeypatch):
    provisioner = ContainerProvisioner()
    provisioner._pvc_enabled = True
    stub_plan_inputs(monkeypatch, provisioner)
    settings = SandboxSettings(
        image=CUSTOM, pull_policy="Always", cpu=1, memory="3Gi", storage="15Gi"
    )
    profile = sandbox_pod_profile(settings, provisioner._image_policy())
    plan = await provisioner._workspace_creation_plan(
        WorkspaceOwner.job(JOB_ID), profile=profile, stateless_creation_generation=None
    )
    assert plan["image"] == CUSTOM
    assert (plan["cpu"], plan["cpu_limit"]) == ("250m", "1000m")
    assert (plan["memory"], plan["memory_limit"]) == ("3Gi", "3Gi")
    assert plan["pvc"]["size"] == "15Gi"
    assert plan["sandbox"] == {"pull_policy": "Always", "storage": "15Gi"}
    assert plan["runtime_policy"]["fuse_enabled"] is False


class _SnapshotDb:
    async def fetchrow(self, *args):
        return None


@pytest.mark.asyncio
async def test_profile_is_resolved_from_the_owner_snapshot(monkeypatch):
    provisioner = ContainerProvisioner()
    provisioner._db = _SnapshotDb()
    resolve = AsyncMock(return_value=SandboxSettings(image=CUSTOM, memory="6Gi"))
    monkeypatch.setattr(provisioner_module, "resolve_sandbox_settings", resolve)
    profile = await provisioner._sandbox_profile(
        WorkspaceOwner.session(THREAD_ID),
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
        image=None,
    )
    resolve.assert_awaited_once_with(provisioner._db, "session", THREAD_ID)
    assert (profile.image, profile.memory, profile.memory_limit) == (
        CUSTOM,
        "6Gi",
        "6Gi",
    )


class _ReservingDb:
    def __init__(self):
        self.reserved = []

    async def fetchrow(self, *args):
        return None

    async def reserve_managed_repository_workspace_creation(self, *args, **kwargs):
        self.reserved.append(kwargs)
        return None

    async def settle_managed_repository_workspace_creation_reservation(
        self, *args, **kwargs
    ):
        return True


@pytest.mark.asyncio
async def test_unreadable_snapshot_fails_closed_before_any_reservation(monkeypatch):
    provisioner = ContainerProvisioner()
    provisioner._k8s_available = True
    provisioner._db = _ReservingDb()
    monkeypatch.setattr(
        provisioner_module,
        "resolve_sandbox_settings",
        AsyncMock(side_effect=RuntimeError("database unavailable")),
    )
    assert await provisioner.create_workspace(WorkspaceOwner.job(JOB_ID)) is False
    assert provisioner._db.reserved == []


def fingerprint_values(provisioner):
    return dict(
        owner=WorkspaceOwner.session(THREAD_ID),
        pod_name="ws-thread-666666667777",
        pvc_name="pvc-ws-thread-666666667777",
        seed_configmap_name=None,
        service_name="ws-thread-666666667777",
        network_tier="internet-only",
        workspace_image=provisioner._workspace_image,
        cpu="500m",
        memory="1Gi",
        cpu_limit="2000m",
        memory_limit="4Gi",
        seed_files={},
        seed_extensions={},
        seed_needs_state=False,
    )


def legacy_pinned_fingerprint(provisioner, values):
    """The pre-A1 pinned contract, field for field (container_provisioner at
    b94e6786c), hashed exactly as _pinned_workspace_provision_fingerprint does.
    """
    pvc_name = values["pvc_name"]
    contract = {
        "render_contract_version": 2,
        "owner_kind": values["owner"].kind,
        "owner_id": values["owner"].id,
        "namespace": provisioner._namespace,
        "pod_name": values["pod_name"],
        "pvc_name": pvc_name,
        "seed_configmap_name": values["seed_configmap_name"],
        "service_name": values["service_name"],
        "network_tier": values["network_tier"],
        "workspace_image": values["workspace_image"],
        "workspace_label_image": provisioner._workspace_image,
        "cpu": values["cpu"],
        "memory": values["memory"],
        "cpu_limit": values["cpu_limit"],
        "memory_limit": values["memory_limit"],
        "pvc_size": provisioner._pvc_size if pvc_name is not None else None,
        "storage_class": provisioner._storage_class if pvc_name is not None else None,
        "fuse_enabled": provisioner._fuse_enabled,
        "fuse_privileged": provisioner._fuse_privileged,
        "workspace_capabilities": [
            "CHOWN",
            "DAC_OVERRIDE",
            "FOWNER",
            "SETGID",
            "SETUID",
            "NET_BIND_SERVICE",
            "SYS_CHROOT",
            "KILL",
            "AUDIT_WRITE",
            *(["SYS_ADMIN"] if provisioner._fuse_enabled else []),
        ],
        "ssh_secret_name": provisioner._ssh_secret_name,
        "seed_files": values["seed_files"],
        "seed_extensions": values["seed_extensions"],
        "seed_needs_state": values["seed_needs_state"],
        "pvc_manifest_contract": 1,
        "seed_configmap_manifest_contract": 1,
        "pod_manifest_contract": 1,
        "service_manifest_contract": 1,
    }
    return hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":"), default=str).encode(
            "utf-8"
        )
    ).hexdigest()


@pytest.mark.parametrize("fuse", ["true", "false"])
@pytest.mark.parametrize("with_pvc", [True, False])
def test_default_profile_keeps_the_legacy_pinned_fingerprint(
    monkeypatch, with_pvc, fuse
):
    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", fuse)
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(SandboxSettings(), provisioner._image_policy())
    values = fingerprint_values(provisioner)
    if not with_pvc:
        values.update(pvc_name=None, service_name=None)
    legacy = legacy_pinned_fingerprint(provisioner, values)
    assert provisioner._pinned_workspace_provision_fingerprint(**values) == legacy
    assert (
        provisioner._pinned_workspace_provision_fingerprint(**values, profile=profile)
        == legacy
    )


def test_templated_profile_changes_the_pinned_fingerprint():
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(
        SandboxSettings(storage="40Gi"), provisioner._image_policy()
    )
    values = fingerprint_values(provisioner)
    assert provisioner._pinned_workspace_provision_fingerprint(
        **values
    ) != provisioner._pinned_workspace_provision_fingerprint(**values, profile=profile)


SERVICE_UID = "55555555-6666-4777-8888-999999999999"


class _TemplatedPinnedDB(_PinnedWorkspaceIntentDB):
    async def fetchrow(self, *args):
        return None


def fake_cluster(provisioner, events):
    """Serve back exactly what the pinned path creates."""
    objects = {}
    builders = {
        "persistent_volume_claim": _pvc_from_manifest,
        "pod": _pod_from_manifest,
        "service": lambda body: _service_from_manifest(body, uid=SERVICE_UID),
    }
    for kind, build in builders.items():

        def create(*, body, _kind=kind, _build=build, **_kwargs):
            events.append((f"create-{_kind}", body))
            objects[_kind] = _build(body)
            return objects[_kind]

        def read(*, _kind=kind, **_kwargs):
            if _kind not in objects:
                missing = Exception("not found")
                missing.status = 404
                raise missing
            return objects[_kind]

        api = provisioner._core_api
        getattr(api, f"create_namespaced_{kind}").side_effect = create
        getattr(api, f"read_namespaced_{kind}").side_effect = read


@pytest.mark.asyncio
async def test_pinned_session_creation_uses_the_templated_snapshot(monkeypatch):
    events = []
    db = _TemplatedPinnedDB(events)
    provisioner = ContainerProvisioner()
    provisioner._db = db
    provisioner._k8s_available = True
    provisioner._core_api = MagicMock()
    provisioner._pvc_enabled = True
    stub_plan_inputs(monkeypatch, provisioner)
    provisioner._wait_for_ready = AsyncMock(return_value="10.42.0.100")
    provisioner._trusted_pod_ssh_identity = AsyncMock(
        return_value=(
            f"k8s-pvc:{provisioner._namespace}:{_TEST_RESOURCE_UID}",
            "SHA256:" + ("A" * 43),
            _TEST_POD_UID,
        )
    )
    fake_cluster(provisioner, events)
    monkeypatch.setattr(
        provisioner_module.workspace_metering,
        "open_interval",
        AsyncMock(return_value=None),
    )
    settings = SandboxSettings(storage="40Gi", memory="6Gi")
    resolve = AsyncMock(return_value=settings)
    monkeypatch.setattr(provisioner_module, "resolve_sandbox_settings", resolve)

    assert await provisioner.create_pinned_thread_workspace(db.THREAD_ID), events

    resolve.assert_awaited_once_with(db, "session", db.THREAD_ID)
    created = {name: body for name, body in events if name.startswith("create-")}
    pvc = created["create-persistent_volume_claim"]
    assert pvc["spec"]["resources"] == {"requests": {"storage": "40Gi"}}
    (workspace,) = [
        container
        for container in created["create-pod"]["spec"]["containers"]
        if container["name"] == "workspace"
    ]
    assert workspace["resources"]["requests"]["memory"] == "6Gi"
    assert workspace["resources"]["limits"]["memory"] == "6Gi"

    reserved = next(payload for name, payload in events if name == "reserve")
    inputs = dict(
        owner=WorkspaceOwner.session(db.THREAD_ID),
        pod_name=reserved["pod_name"],
        pvc_name=reserved["pvc_name"],
        seed_configmap_name=reserved["seed_configmap_name"],
        service_name=reserved["service_name"],
        network_tier="internet-only",
        workspace_image=provisioner._workspace_image,
        cpu="500m",
        memory="6Gi",
        cpu_limit="2000m",
        memory_limit="6Gi",
        seed_files={},
        seed_extensions={},
        seed_needs_state=False,
    )
    profile = sandbox_pod_profile(settings, provisioner._image_policy())
    templated = provisioner._pinned_workspace_provision_fingerprint(
        **inputs, profile=profile
    )
    assert reserved["manifest_fingerprint"] == templated
    assert templated != provisioner._pinned_workspace_provision_fingerprint(**inputs)
