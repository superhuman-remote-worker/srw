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


def _gate_test_fixture():
    """Inputs shared by the denied/allowed cases below: a real mount target
    (``nc_session_folder``) and a mocked session-mount builder, so a payload
    is possible at all. Only ``container_denies_fuse``'s mocked answer varies
    between the two parametrize cases — nothing else."""
    session_mount = AsyncMock(return_value={"mount_kind": "session_folder"})
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
    return session_mount, dependencies, metadata, thread


@pytest.mark.asyncio
@pytest.mark.parametrize("denies_fuse", [True, False])
async def test_fuse_gate_alone_decides_the_cloud_mount(monkeypatch, denies_fuse):
    """Denied and allowed share identical inputs (a real mount target plus a
    working session-mount builder) and differ ONLY in what
    container_denies_fuse answers: denied -> None and the builder is never
    called; allowed -> a non-None payload and the builder runs. This proves
    the gate itself, not an unrelated "nothing to mount" fallthrough, decides
    the outcome."""
    session_mount, dependencies, metadata, thread = _gate_test_fixture()
    monkeypatch.setattr(
        agent_cloud_mounts,
        "container_denies_fuse",
        AsyncMock(return_value=denies_fuse),
    )
    monkeypatch.setattr(
        agent_cloud_mounts, "_build_rclone_session_mount", session_mount
    )

    payload = await agent_cloud_mounts._build_agent_cloud_mount(
        thread, mount_rows=[], metadata=metadata, dependencies=dependencies
    )

    if denies_fuse:
        assert payload is None
        session_mount.assert_not_awaited()
    else:
        assert payload is not None
        assert payload["mounts"] == [{"mount_kind": "session_folder"}]
        session_mount.assert_awaited_once()
