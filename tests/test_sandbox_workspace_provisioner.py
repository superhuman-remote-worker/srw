"""ContainerProvisioner resolves frozen container settings for every creation."""

from unittest.mock import AsyncMock

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
async def test_empty_template_plan_is_byte_identical_to_the_legacy_plan(monkeypatch):
    provisioner = ContainerProvisioner()
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


def test_default_profile_keeps_the_pinned_fingerprint():
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(SandboxSettings(), provisioner._image_policy())
    values = fingerprint_values(provisioner)
    assert provisioner._pinned_workspace_provision_fingerprint(
        **values
    ) == provisioner._pinned_workspace_provision_fingerprint(**values, profile=profile)


def test_templated_profile_changes_the_pinned_fingerprint():
    provisioner = ContainerProvisioner()
    profile = sandbox_pod_profile(
        SandboxSettings(storage="40Gi"), provisioner._image_policy()
    )
    values = fingerprint_values(provisioner)
    assert provisioner._pinned_workspace_provision_fingerprint(
        **values
    ) != provisioner._pinned_workspace_provision_fingerprint(**values, profile=profile)
