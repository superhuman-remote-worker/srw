"""A custom container without FUSE never receives an rclone mount."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from orchestrator.services import agent_cloud_mounts
from orchestrator.services import sandbox_workspace_settings as settings_module
from orchestrator.services.sandbox_workspace_settings import (
    SandboxSettings,
    container_denies_fuse,
)

THREAD_ID = "66666666-7777-4888-8999-aaaaaaaaaaaa"
CUSTOM = "registry.example/team/workspace:1"


@pytest.mark.asyncio
async def test_thread_without_snapshot_is_never_denied():
    assert await container_denies_fuse(SimpleNamespace(), {"id": THREAD_ID}) is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "image,switch,denied",
    [(CUSTOM, "false", True), (CUSTOM, "true", False), (None, "false", False)],
)
async def test_custom_image_is_denied_unless_the_switch_allows_it(
    monkeypatch, image, switch, denied
):
    # Deterministic policy inputs: a fixed installation image (never CUSTOM's
    # repository), FUSE on at the installation, and no extra trusted repos —
    # so the only thing varying across parametrize cases is the switch.
    monkeypatch.setenv(
        "WORKSPACE_IMAGE", "ghcr.io/superhuman-remote-worker/srw-workspace:latest"
    )
    monkeypatch.setenv("WORKSPACE_FUSE_ENABLED", "true")
    monkeypatch.delenv("WORKSPACE_TRUSTED_IMAGE_REPOSITORIES", raising=False)
    monkeypatch.setenv("WORKSPACE_CUSTOM_IMAGES_PRIVILEGED", switch)
    monkeypatch.setattr(
        settings_module,
        "resolve_sandbox_settings",
        AsyncMock(return_value=SandboxSettings(image=image)),
    )
    thread = {"id": THREAD_ID, "execution_harness_adapter": "srw/v1"}
    assert await container_denies_fuse(object(), thread) is denied


@pytest.mark.asyncio
async def test_denied_container_gets_no_cloud_mount(monkeypatch):
    monkeypatch.setattr(
        agent_cloud_mounts, "container_denies_fuse", AsyncMock(return_value=True)
    )
    dependencies = agent_cloud_mounts.AgentCloudMountDependencies(
        store=SimpleNamespace(),
        cloud_router=None,
        cloud_tasks=None,
        is_protected_cloud_mode_enabled=lambda: False,
        cloud_workspace_driver=lambda: "rclone_mount",
        slugify_mount_name=lambda name: name,
    )
    metadata = {"workspace_container": {"status": "ready", "pod_ip": "10.0.0.5"}}
    thread = {"id": THREAD_ID, "execution_harness_adapter": "srw/v1"}
    assert (
        await agent_cloud_mounts._build_agent_cloud_mount(
            thread, mount_rows=[], metadata=metadata, dependencies=dependencies
        )
        is None
    )


@pytest.mark.asyncio
async def test_allowed_container_reaches_the_mount_builder(monkeypatch):
    """Companion to test_denied_container_gets_no_cloud_mount: proves the FUSE
    gate — not some other refusal — is what returned None there. Same thread,
    same metadata, same dependencies shape; only container_denies_fuse's
    answer flips, and the call now reaches the mount builder and returns a
    non-None payload."""
    monkeypatch.setattr(
        agent_cloud_mounts, "container_denies_fuse", AsyncMock(return_value=False)
    )
    session_mount = AsyncMock(return_value={"mount_kind": "session_folder"})
    monkeypatch.setattr(
        agent_cloud_mounts, "_build_rclone_session_mount", session_mount
    )
    dependencies = agent_cloud_mounts.AgentCloudMountDependencies(
        store=SimpleNamespace(),
        cloud_router=None,
        cloud_tasks=None,
        is_protected_cloud_mode_enabled=lambda: False,
        cloud_workspace_driver=lambda: "rclone_mount",
        slugify_mount_name=lambda name: name,
    )
    metadata = {"workspace_container": {"status": "ready", "pod_ip": "10.0.0.5"}}
    thread = {
        "id": THREAD_ID,
        "execution_harness_adapter": "srw/v1",
        "nc_session_folder": "nextcloud:1",
    }
    payload = await agent_cloud_mounts._build_agent_cloud_mount(
        thread, mount_rows=[], metadata=metadata, dependencies=dependencies
    )
    session_mount.assert_awaited_once()
    assert payload is not None
    assert payload["mounts"] == [{"mount_kind": "session_folder"}]
