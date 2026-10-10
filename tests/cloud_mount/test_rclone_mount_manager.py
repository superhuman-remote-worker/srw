from __future__ import annotations

import copy
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import shared.runtime.services.cloud_mount as cloud_mount_module
from shared.runtime.services.cloud_mount import (
    RcloneMountCleanFailure,
    RcloneMountError,
    RcloneMountManager,
)
from shared.runtime.services.keycloak_token import BearerToken


class FakeRemoteBackend:
    def __init__(self, *, root: str | None = None) -> None:
        self.files: dict[str, str] = {}
        self.commands: list[tuple[str, int]] = []
        self.resource_operations: list[str] = []
        self.outputs_by_script: dict[str, str] = {}
        self.claim_resource_fenced = False
        self.claim_resource_retired = False
        if root is not None:
            self.root = root

    def resolve_home_path(self, relative_path: str) -> str:
        return f"/home/agent-host/{relative_path}"

    def write_home_file(self, relative_path: str, content: str | bytes) -> None:
        self.files[relative_path] = (
            content.decode("utf-8") if isinstance(content, bytes) else content
        )

    def exec_command(self, command: str, timeout: int = 30) -> str:
        self.commands.append((command, timeout))
        for script_name, output in self.outputs_by_script.items():
            if f"/{script_name}" in command:
                return output
        return "__SRW_RCLONE_MOUNT_OK__\n"

    def exec_claim_resource(
        self,
        command: str,
        timeout: int = 30,
        *,
        operation: str = "workspace resource mutation",
    ) -> str:
        if self.claim_resource_retired:
            raise RcloneMountError("claim resource retired")
        self.resource_operations.append(operation)
        return self.exec_command(command, timeout=timeout)

    def retire_claim_resource_owner(self) -> None:
        self.claim_resource_retired = True


class FakeFencedRemoteBackend(FakeRemoteBackend):
    def __init__(self, *, root: str | None = None) -> None:
        super().__init__(root=root)
        self.claim_resource_fenced = True


def _cloud_mount_cfg() -> dict:
    return {
        "version": 1,
        "driver": "rclone",
        "cloud_root": "/cloud",
        "workspace_entry": "cloud",
        "mounts": [
            {
                "mount_id": "legacy-session",
                "mount_kind": "session_folder",
                "target_path": "/cloud/home",
                "workspace_name": "home",
                "backend": "nextcloud",
                "source": {
                    "type": "webdav",
                    "config": {
                        "url": "https://nc.test/remote.php/dav/files/agent/session/",
                        "vendor": "nextcloud",
                        "user": "agent-service",
                    },
                },
                "auth": {"type": "basic", "password": "app-pass"},
                "cache": {"vfs_cache_max_size": "10G", "hard_cache_limit": "20G"},
            }
        ],
    }


def test_starts_rclone_mount_and_installs_workspace_symlink():
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )

    manager._start_all_sync()

    assert manager.active is True
    assert len(manager.mounts) == 1
    mount_scripts = [
        body
        for path, body in backend.files.items()
        if path.endswith("mount_srw-thread-1-home.sh")
    ]
    assert len(mount_scripts) == 1
    script = mount_scripts[0]
    assert "rclone --config" in script
    assert "config create srw-thread-1-home webdav" in script
    assert "--vfs-cache-mode full" in script
    assert "--vfs-cache-max-size 10G" in script
    assert "cat srw-thread-1-home:.cloudignore" in script
    assert "timeout 15 rclone --config" in script
    assert "MOUNT_ARGS+=(--exclude-from" in script
    assert "fusermount3 -u /cloud/home" in script
    assert "/cloud/home" in script
    assert "app-pass" in script

    link_scripts = [
        body
        for path, body in backend.files.items()
        if path.endswith("install_cloud_links.sh")
    ]
    assert len(link_scripts) == 1
    assert "ln -sfn /cloud/home" in link_scripts[0]
    assert "${workspace}/cloud" in link_scripts[0]


def test_config_create_shields_credential_that_looks_like_a_flag():
    """Regression: a reader credential that begins with ``-`` must not be
    parsed by rclone (cobra) as a flag. ``config create`` puts its flags
    before a POSIX ``--`` end-of-options marker, after which every token is a
    positional ``key value`` pair — so a ``-``-leading ``pass`` is safe.

    Without the marker rclone aborts with "unknown shorthand flag" and the
    whole mount fails (observed live: reader credential ``-Yi9OE…``)."""
    dash_pass = "-Yi9OE14rYnaHKdUk7EkBtnjT1-LPw8A"
    cfg = _cloud_mount_cfg()
    cfg["mounts"][0]["auth"]["password"] = dash_pass
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )

    manager._start_all_sync()

    script = next(
        body
        for path, body in backend.files.items()
        if path.endswith("mount_srw-thread-1-home.sh")
    )
    create_line = next(ln for ln in script.splitlines() if "config create" in ln)
    # Flags precede the end-of-options marker; the credential follows it.
    i_obscure = create_line.index("--obscure")
    i_sep = create_line.index(" -- ")
    i_pass = create_line.index(dash_pass)
    assert i_obscure < i_sep < i_pass, create_line


def test_cache_flags_are_gated_by_rclone_mount_help():
    cfg = _cloud_mount_cfg()
    cfg["mounts"][0]["cache"]["vfs_cache_min_free_space"] = "5G"
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )

    manager._start_all_sync()

    script = next(
        body
        for path, body in backend.files.items()
        if path.endswith("mount_srw-thread-1-home.sh")
    )
    mount_array = script.split("MOUNT_ARGS=(", 1)[1].split(")\n", 1)[0]
    assert "--vfs-cache-min-free-space" not in mount_array
    assert "MOUNT_HELP=" in script
    assert "grep -Fq --" in script
    assert "append_mount_flag --vfs-cache-min-free-space 5G" in script


def test_workspace_link_uses_remote_backend_root_when_available():
    backend = FakeRemoteBackend(root="/home/agent-host/workspace")
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )

    manager._start_all_sync()

    link_script = next(
        body
        for path, body in backend.files.items()
        if path.endswith("install_cloud_links.sh")
    )
    assert "workspace=/home/agent-host/workspace" in link_script
    assert "workspace=/workspace" not in link_script


def test_mount_script_applies_default_filters_and_read_only_flag():
    cfg = _cloud_mount_cfg()
    cfg["default_ignores"] = ["Photos/", "*.iso"]
    cfg["mounts"][0]["access"] = "read_only"
    cfg["mounts"][0]["filters"] = {"exclude": ["Videos/"]}
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )

    manager._start_all_sync()

    script = next(
        body
        for path, body in backend.files.items()
        if path.endswith("mount_srw-thread-1-home.sh")
    )
    assert "compile_cloudignore" in script
    assert "Photos/" in script
    assert "*.iso" in script
    assert "Videos/" in script
    assert "--read-only" in script


def test_cache_limit_message_blocks_when_hard_limit_reached():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["cloud_cache_usage.sh"] = (
        "__SRW_RCLONE_CACHE_USAGE__\tlegacy-session\thome\t/cloud/home\t"
        "/home/agent-host/.cache/srw/rclone/thread-12345678/home/vfs-cache\t"
        "21474836480\t21474836480\t20G\tyes\n"
        "__SRW_RCLONE_MOUNT_OK__\n"
    )
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    manager._start_all_sync()

    message = manager.cache_limit_message()

    assert message is not None
    assert "Cloud cache guard" in message
    assert "home at /cloud/home" in message
    assert "20.0 GiB used" in message


def test_status_reports_cache_and_rc_stats_without_credentials():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["cloud_mount_status.sh"] = (
        "__SRW_RCLONE_CACHE_USAGE__\tlegacy-session\thome\t/cloud/home\t"
        "/home/agent-host/.cache/srw/rclone/thread-12345678/home/vfs-cache\t"
        "1048576\t21474836480\t20G\tyes\n"
        '__SRW_RCLONE_RC_CORE__\tlegacy-session\t{"bytes":1048576}\n'
        '__SRW_RCLONE_RC_VFS__\tlegacy-session\t{"cacheSize":1048576}\n'
        "__SRW_RCLONE_MOUNT_OK__\n"
    )
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    manager._start_all_sync()

    status = manager.status()

    assert "Cloud mount status" in status
    assert "mounted, cache 1.0 MiB / 20.0 GiB hard limit" in status
    assert 'core/stats: {"bytes":1048576}' in status
    assert manager.mounts[0].rc_pass not in status


def test_refresh_vfs_issues_vfs_refresh_not_forget():
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    manager._start_all_sync()
    manager.refresh_vfs()
    scripts = [b for p, b in backend.files.items() if "vfs_refresh" in p]
    assert scripts, "expected a vfs_refresh script"
    s = scripts[0]
    assert "vfs/refresh" in s
    assert "recursive=true" in s
    assert "vfs/forget" not in s  # forget does NOT flush file content (design §11.2)


_IDLE_CORE_STATS = {"bytes": 0, "errors": 0, "transfers": 0}
_IDLE_VFS_STATS = {"diskCache": {"uploadsQueued": 0, "uploadsInProgress": 0}}


def _terminal_drain_status_returncode(core, vfs) -> int:
    core_json = core if isinstance(core, str) else json.dumps(core)
    vfs_json = vfs if isinstance(vfs, str) else json.dumps(vfs)
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            cloud_mount_module._TERMINAL_DRAIN_STATUS_PY,
            core_json,
            vfs_json,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    return result.returncode


@pytest.mark.parametrize("transferring", [pytest.param(None, id="omitted"), []])
def test_terminal_drain_accepts_rclone_idle_transfer_shapes(transferring):
    core = dict(_IDLE_CORE_STATS)
    if transferring is not None:
        core["transferring"] = transferring

    assert _terminal_drain_status_returncode(core, _IDLE_VFS_STATS) == 0


@pytest.mark.parametrize(
    "transferring",
    [[{"name": "pending"}], None, {}, "idle", 0, False],
)
def test_terminal_drain_rejects_active_or_malformed_transfer_shapes(transferring):
    core = {**_IDLE_CORE_STATS, "transferring": transferring}

    assert _terminal_drain_status_returncode(core, _IDLE_VFS_STATS) == 1


@pytest.mark.parametrize(
    "core",
    [
        {},
        {"error": "permission denied", "path": "core/stats", "status": 500},
        [],
        "{invalid-json",
        {"bytes": 0, "errors": 0},
        {"bytes": 0, "errors": 0, "transfers": False},
        {"bytes": 0, "errors": "0", "transfers": 0},
        {"bytes": 0.0, "errors": 0, "transfers": 0},
        {"bytes": 0, "errors": -1, "transfers": 0},
    ],
)
def test_terminal_drain_rejects_invalid_core_responses(core):
    assert _terminal_drain_status_returncode(core, _IDLE_VFS_STATS) == 1


@pytest.mark.parametrize("counter", ["uploadsQueued", "uploadsInProgress"])
@pytest.mark.parametrize("value", [1, -1, "0", 0.0, 0.5, False, None, {}])
def test_terminal_drain_rejects_non_idle_or_malformed_vfs_counters(counter, value):
    disk_cache = dict(_IDLE_VFS_STATS["diskCache"])
    disk_cache[counter] = value

    assert (
        _terminal_drain_status_returncode(_IDLE_CORE_STATS, {"diskCache": disk_cache})
        == 1
    )


@pytest.mark.parametrize(
    "vfs",
    [
        {},
        {"diskCache": None},
        {"diskCache": []},
        {"diskCache": {"uploadsQueued": 0}},
        {"diskCache": {"uploadsInProgress": 0}},
        [],
        "{invalid-json",
    ],
)
def test_terminal_drain_rejects_invalid_vfs_responses(vfs):
    assert _terminal_drain_status_returncode(_IDLE_CORE_STATS, vfs) == 1


def test_terminal_unmount_requires_successful_full_rc_probes_before_quit():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )
    state = manager._state_for_mount(_cloud_mount_cfg()["mounts"][0], 0)
    script = manager._terminal_unmount_script(state, drain=True)

    syntax = subprocess.run(
        ["bash", "-n"], input=script, text=True, capture_output=True, check=False
    )

    assert syntax.returncode == 0, syntax.stderr
    assert "core/stats 2>/dev/null || true" not in script
    assert "vfs/stats 2>/dev/null || true" not in script
    assert "short=true" not in script
    assert "core/stats 2>/dev/null) &&" in script
    assert "vfs/stats 2>/dev/null) &&" in script
    drain_ack = script.index('[ "$_srw_drained" = yes ] || exit 89')
    quit_call = script.index("core/quit")
    assert drain_ack < quit_call


# ------------------------------------------------------- Keycloak bearer auth


def _opencloud_mount_cfg() -> dict:
    return {
        "version": 1,
        "driver": "rclone",
        "cloud_root": "/cloud",
        "workspace_entry": "cloud",
        "mounts": [
            {
                "mount_id": "legacy-session",
                "mount_kind": "session_folder",
                "target_path": "/cloud/home",
                "workspace_name": "home",
                "backend": "opencloud",
                "source": {
                    "type": "webdav",
                    "config": {
                        "url": "https://oc.test/dav/spaces/drive-1/sessions/t1/",
                        "vendor": "infinitescale",
                    },
                },
                "auth": {
                    "type": "keycloak_client_credentials",
                    "issuer": "https://kc.test/realms/srw",
                    "client_id": "opencloud-orchestrator",
                    "client_secret": "kc-secret",
                },
                "cache": {"vfs_cache_max_size": "10G", "hard_cache_limit": "20G"},
                "min_rclone_version": "1.70.0",
            }
        ],
    }


class FakeTokenClient:
    instances: list["FakeTokenClient"] = []

    def __init__(
        self,
        *,
        issuer: str,
        client_id: str,
        client_secret: str,
        target_user_sub: str | None = None,
        **_: object,
    ) -> None:
        self.issuer = issuer
        self.client_id = client_id
        self.client_secret = client_secret
        self.target_user_sub = target_user_sub
        self.minted = 0
        self.closed = False
        self._token_expires_at = 10**9  # far-future monotonic deadline
        FakeTokenClient.instances.append(self)

    async def get_bearer(self, force_refresh: bool = False) -> BearerToken:
        self.minted += 1
        return BearerToken(
            token=f"tok-{self.minted}", expires_at=self._token_expires_at
        )

    async def aclose(self) -> None:
        self.closed = True


@pytest.fixture()
def fake_token_client(monkeypatch):
    FakeTokenClient.instances = []
    monkeypatch.setattr(cloud_mount_module, "KeycloakTokenClient", FakeTokenClient)
    yield FakeTokenClient
    FakeTokenClient.instances = []


@pytest.mark.asyncio
async def test_keycloak_mount_uses_bearer_token_command(fake_token_client):
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )

    await manager.start_all()
    try:
        script = next(
            body
            for path, body in backend.files.items()
            if path.endswith("mount_srw-thread-1-home.sh")
        )
        state = manager.mounts[0]
        assert state.uses_keycloak_auth is True
        # rclone remote authenticates via the helper, not a static credential.
        assert "bearer_token_command" in script
        assert state.token_helper_path in script
        assert "tok-1" in script  # initial token seeded into the token file
        assert f"chmod 600 {state.token_path}" in script
        assert "pass " not in script.split("config create", 1)[1].split("\n", 1)[0]
        # The Keycloak client secret must never reach the workspace runtime.
        for body in backend.files.values():
            assert "kc-secret" not in body
        for command, _timeout in backend.commands:
            assert "kc-secret" not in command
        # Version preflight gates old runtimes with a clear error.
        assert "required_ver=1.70.0" in script
        assert "sort -V" in script
        # Refresh loop is running for the keycloak mount.
        assert manager._refresh_task is not None
        assert fake_token_client.instances[0].minted == 1
    finally:
        await manager.aclose()

    assert fake_token_client.instances[0].closed is True
    assert manager._refresh_task is None


@pytest.mark.asyncio
async def test_keycloak_auth_missing_fields_raises(fake_token_client):
    cfg = _opencloud_mount_cfg()
    del cfg["mounts"][0]["auth"]["client_secret"]
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg,
        workspace_backend=FakeRemoteBackend(),
        workspace_root=Path("/home/agent-host/workspace"),
    )
    with pytest.raises(RcloneMountError, match="issuer/client_id/client_secret"):
        await manager.start_all()


def test_mount_script_requires_prepared_token():
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=FakeRemoteBackend(),
        workspace_root=Path("/home/agent-host/workspace"),
    )
    # _start_all_sync without the async token prep must refuse keycloak mounts.
    with pytest.raises(RcloneMountError, match="no initial bearer token"):
        manager._start_all_sync()


def test_pinned_push_token_preserves_unique_sftp_staging():
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    manager._initial_tokens[0] = "tok-seed"
    manager._start_all_sync()
    state = manager.mounts[0]

    manager._push_token_sync(state, "tok-fresh")

    tmp_paths = [path for path in backend.files if "/bearer.token.new." in path]
    assert len(tmp_paths) == 1
    assert backend.files[tmp_paths[0]] == "tok-fresh\n"
    last_command, _timeout = backend.commands[-1]
    assert "chmod 600" in last_command
    assert "mv -f" in last_command
    assert state.token_path in last_command
    assert "tok-fresh" not in last_command
    assert backend.resource_operations[-1] != "publish bearer token for legacy-session"


def test_pinned_push_token_uses_unique_sftp_path_each_time():
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    manager._initial_tokens[0] = "tok-seed"
    manager._start_all_sync()
    state = manager.mounts[0]

    manager._push_token_sync(state, "tok-fresh-1")
    manager._push_token_sync(state, "tok-fresh-2")

    tmp_paths = {path for path in backend.files if "/bearer.token.new." in path}
    assert len(tmp_paths) == 2


def test_stateless_push_token_is_wholly_inside_one_fenced_command():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    state = manager._state_for_mount(_opencloud_mount_cfg()["mounts"][0], 0)

    manager._push_token_sync(state, "tok-fresh")

    command, _timeout = backend.commands[-1]
    assert "mktemp" in command
    assert "base64 -d" in command
    assert "chmod 600" in command
    assert "mv -f" in command
    assert state.token_path in command
    assert "tok-fresh" not in command
    assert not any("bearer.token.new" in path for path in backend.files)
    assert backend.resource_operations == ["publish bearer token for legacy-session"]


@pytest.mark.asyncio
async def test_refreshed_token_is_used_by_later_restart(fake_token_client):
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )

    await manager.start_all()
    try:
        await manager._refresh_keycloak_tokens_once()
        manager.restart_mount("legacy-session")

        mount_script = next(
            body
            for path, body in backend.files.items()
            if path.endswith("mount_srw-thread-1-home.sh")
        )
        assert "tok-2" in mount_script
        assert "tok-1" not in mount_script
        assert manager._token_for_index(0) == "tok-2"
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_partial_multi_mount_start_rolls_back_in_reverse_order():
    cfg = _cloud_mount_cfg()
    second = copy.deepcopy(cfg["mounts"][0])
    second.update(
        {
            "mount_id": "second-mount",
            "target_path": "/cloud/second",
            "workspace_name": "second",
        }
    )
    cfg["mounts"].append(second)
    backend = FakeRemoteBackend()
    backend.outputs_by_script["mount_srw-thread-1-second.sh"] = (
        "__SRW_RCLONE_MOUNT_FAILED__ rc=1\n"
    )
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )

    with pytest.raises(RcloneMountError):
        await manager.start_all()

    commands = [command for command, _timeout in backend.commands]
    assert len(commands) == 4
    assert "mount_srw-thread-1-home.sh" in commands[0]
    assert "mount_srw-thread-1-second.sh" in commands[1]
    assert "unmount_srw-thread-1-second.sh" in commands[2]
    assert "unmount_srw-thread-1-home.sh" in commands[3]
    assert manager.active is False
    assert manager.mounts == []
    assert manager._mounts_by_id == {}
    assert manager._mount_index_by_id == {}


def test_pinned_unmount_preserves_historical_unconditional_cleanup():
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    manager._initial_tokens[0] = "tok-seed"
    manager._start_all_sync()
    state = manager.mounts[0]

    script = manager._unmount_script(state)

    assert state.token_path in script
    assert state.token_helper_path in script
    assert "core/quit" in script
    assert "resident.identity" not in script
    assert "kill -9" in script
    assert "_srw_pid_matches" not in script


def test_stateless_unmount_requires_exact_identity_and_pid_cmdline():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    state = manager._state_for_mount(_opencloud_mount_cfg()["mounts"][0], 0)

    script = manager._unmount_script(state)

    assert state.identity_file in script
    assert state.resident_spec_digest in script
    assert state.resident_generation in script
    assert "_srw_pid_matches" in script
    assert 'basename "$_srw_exe"' in script
    assert "timeout 10 rclone rc" in script
    assert "kill -9" in script


def _resident_unmount_runtime(
    tmp_path: Path, *, resident_pid: int
) -> tuple[str, dict[str, str]]:
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=tmp_path / "workspace",
    )
    original = manager._state_for_mount(_opencloud_mount_cfg()["mounts"][0], 0)
    state_dir = tmp_path / "resident"
    target = tmp_path / "target"
    state_dir.mkdir()
    target.mkdir()
    state = replace(
        original,
        state_dir=str(state_dir),
        target_path=str(target),
        config_path=str(state_dir / "rclone.conf"),
        pid_file=str(state_dir / "rclone.pid"),
        identity_file=str(state_dir / "resident.identity"),
        token_path=str(state_dir / "bearer.token"),
        token_helper_path=str(state_dir / "bearer-helper.sh"),
    )
    Path(state.identity_file).write_text(
        "|".join(
            (
                "1",
                state.resident_spec_digest,
                "active",
                state.resident_generation,
                str(resident_pid),
                state.rc_pass,
            )
        )
        + "\n"
    )
    Path(state.pid_file).write_text(f"{resident_pid}\n")
    Path(state.token_path).write_text("token\n")
    Path(state.token_helper_path).write_text("helper\n")
    mounted = tmp_path / "mounted"
    mounted.touch()
    events = tmp_path / "events"
    bash_env = tmp_path / "bash-env"
    bash_env.write_text(
        """
# Model the observed ENOTCONN defect: mountpoint(1) lies because its target
# stat fails, while the kernel mount table still carries the stale FUSE mount.
mountpoint() { return 1; }
findmnt() { [ -e "$SRW_TEST_MOUNTED" ]; }
fusermount3() {
  printf 'fusermount3 %s\\n' "$*" >> "$SRW_TEST_EVENTS"
  rm -f "$SRW_TEST_MOUNTED"
}
fusermount() {
  printf 'fusermount %s\\n' "$*" >> "$SRW_TEST_EVENTS"
  rm -f "$SRW_TEST_MOUNTED"
}
sleep() { :; }
timeout() { shift; "$@"; }
export -f mountpoint findmnt fusermount3 fusermount sleep timeout
"""
    )
    script = manager._unmount_script(state)
    env = {
        **os.environ,
        "BASH_ENV": str(bash_env),
        "SRW_TEST_MOUNTED": str(mounted),
        "SRW_TEST_EVENTS": str(events),
    }
    return script, env


def _zero_proof_unmount_runtime(
    tmp_path: Path,
    *,
    creating_identity_without_pid: bool,
) -> tuple[RcloneMountManager, object, Path]:
    """Build a real-shell strict unmount case with no usable PID identity."""

    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=FakeFencedRemoteBackend(),
        workspace_root=tmp_path / "workspace",
    )
    original = manager._state_for_mount(_opencloud_mount_cfg()["mounts"][0], 0)
    state_dir = tmp_path / "resident"
    target = tmp_path / "target"
    state_dir.mkdir()
    target.mkdir()
    state = replace(
        original,
        state_dir=str(state_dir),
        target_path=str(target),
        config_path=str(state_dir / "rclone.conf"),
        pid_file=str(state_dir / "rclone.pid"),
        identity_file=str(state_dir / "resident.identity"),
        token_path=str(state_dir / "bearer.token"),
        token_helper_path=str(state_dir / "bearer-helper.sh"),
    )
    if creating_identity_without_pid:
        Path(state.identity_file).write_text(
            "|".join(
                (
                    "1",
                    state.resident_spec_digest,
                    "creating",
                    state.resident_generation,
                    "0",
                    state.rc_pass,
                )
            )
            + "\n"
        )
    script_path = tmp_path / "unmount-zero-proof.sh"
    script_path.write_text(manager._unmount_script(state))
    return manager, state, script_path


@pytest.mark.parametrize(
    "creating_identity_without_pid",
    [False, True],
    ids=["missing-identity", "creating-pid-zero-no-pidfile"],
)
def test_stateless_unmount_refuses_live_exact_process_without_pid_identity(
    tmp_path, creating_identity_without_pid
):
    _manager, state, script_path = _zero_proof_unmount_runtime(
        tmp_path,
        creating_identity_without_pid=creating_identity_without_pid,
    )
    resident = subprocess.Popen(
        [
            "rclone",
            "-c",
            "while :; do sleep 1; done",
            "mount",
            state.config_path,
            state.target_path,
            state.rc_addr,
        ],
        executable="/bin/bash",
        start_new_session=True,
    )
    try:
        for _ in range(50):
            if (
                Path(f"/proc/{resident.pid}/cmdline")
                .read_bytes()
                .startswith(b"rclone\0")
            ):
                break
            time.sleep(0.01)
        result = subprocess.run(
            ["bash", str(script_path)],
            text=True,
            capture_output=True,
        )

        assert result.returncode == 85, result.stderr
        assert resident.poll() is None
        if creating_identity_without_pid:
            assert Path(state.identity_file).exists()
    finally:
        os.killpg(resident.pid, signal.SIGKILL)
        resident.wait(timeout=2)


def test_stateless_dead_resident_pid_unmounts_stale_target(tmp_path):
    dead_pid = 2_147_483_647
    assert not Path(f"/proc/{dead_pid}").exists()
    script, env = _resident_unmount_runtime(tmp_path, resident_pid=dead_pid)
    script_path = tmp_path / "unmount-dead.sh"
    script_path.write_text(script)

    syntax = subprocess.run(
        ["bash", "-n", str(script_path)], env=env, text=True, capture_output=True
    )
    result = subprocess.run(
        ["bash", str(script_path)], env=env, text=True, capture_output=True
    )

    assert syntax.returncode == 0, syntax.stderr
    assert result.returncode == 0, result.stderr
    assert "__SRW_RCLONE_MOUNT_OK__" in result.stdout
    assert "fusermount3 -u" in (tmp_path / "events").read_text()
    assert not (tmp_path / "mounted").exists()
    assert not (tmp_path / "resident" / "resident.identity").exists()


def test_stateless_reused_live_pid_refuses_kill_and_unmount(tmp_path):
    live_mismatched_pid = os.getpid()
    assert Path(f"/proc/{live_mismatched_pid}").exists()
    script, env = _resident_unmount_runtime(tmp_path, resident_pid=live_mismatched_pid)
    script_path = tmp_path / "unmount-reused.sh"
    script_path.write_text(script)

    syntax = subprocess.run(
        ["bash", "-n", str(script_path)], env=env, text=True, capture_output=True
    )
    result = subprocess.run(
        ["bash", str(script_path)], env=env, text=True, capture_output=True
    )

    assert syntax.returncode == 0, syntax.stderr
    assert result.returncode == 85
    assert (tmp_path / "mounted").exists()
    assert not (tmp_path / "events").exists()
    assert (tmp_path / "resident" / "resident.identity").exists()


def test_stateless_heal_requires_conclusive_unmount_ack_before_remount():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )
    candidate = manager._state_for_mount(_cloud_mount_cfg()["mounts"][0], 0)
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_HEAL__\t"
        f"{candidate.resident_spec_digest}\t{'e' * 32}\t999999999\t"
        "residentPass_123456789012345\n"
    )
    backend.outputs_by_script["unmount_srw-thread-1-home.sh"] = (
        "stale FUSE target remains\n"
    )

    with pytest.raises(RcloneMountError, match="stale FUSE target remains"):
        manager._start_all_sync()

    assert not any(
        operation.endswith("mount_srw-thread-1-home.sh")
        and not operation.endswith("unmount_srw-thread-1-home.sh")
        for operation in backend.resource_operations
    )


def test_generated_pinned_and_stateless_unmount_scripts_parse_in_bash(tmp_path):
    pinned = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=FakeRemoteBackend(),
        workspace_root=tmp_path,
    )
    fenced = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=FakeFencedRemoteBackend(),
        workspace_root=tmp_path,
    )
    for name, manager in (("pinned", pinned), ("stateless", fenced)):
        state = manager._state_for_mount(_opencloud_mount_cfg()["mounts"][0], 0)
        result = subprocess.run(
            ["bash", "-n"],
            input=manager._unmount_script(state),
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, f"{name}: {result.stderr}"


def test_skip_workspace_links_omits_symlink_install():
    """Protected mode sets skip_workspace_links; the overlay owns the
    workspace/cloud symlink (pointing at the merged view), so the plain
    rclone mount must not install its own install_cloud_links.sh."""
    cfg = _cloud_mount_cfg()
    cfg["skip_workspace_links"] = True
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    manager._start_all_sync()
    link_scripts = [p for p in backend.files if p.endswith("install_cloud_links.sh")]
    assert link_scripts == []  # overlay owns the symlink in protected mode


@pytest.mark.asyncio
async def test_stateless_successor_adopts_healthy_resident_after_local_detach():
    first_backend = FakeFencedRemoteBackend()
    first_backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_HEAL__\n"
    )
    first = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=first_backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    await first.start_all()
    resident = first.mounts[0]
    commands_before_detach = list(first_backend.commands)
    operations_before_detach = list(first_backend.resource_operations)

    await first.detach_for_handoff()

    assert first_backend.commands == commands_before_detach
    assert first_backend.resource_operations == operations_before_detach
    assert first_backend.claim_resource_retired is True

    second_backend = FakeFencedRemoteBackend()
    second_backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_ADOPTED__\t"
        f"{resident.resident_spec_digest}\t{resident.resident_generation}\t"
        f"321\t{resident.rc_pass}\n"
    )
    second = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=second_backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    await second.start_all()
    try:
        assert second.mounts[0].resident_generation == resident.resident_generation
        assert second.mounts[0].rc_pass == resident.rc_pass
        assert not any(
            Path(path).name.startswith("mount_") for path in second_backend.files
        )
        probe = next(
            body
            for path, body in second_backend.files.items()
            if path.endswith("probe_srw-thread-1-home.sh")
        )
        assert "timeout 15 find" in probe
        assert "-mindepth 1 -maxdepth 1" in probe
        assert "_srw_pid_matches" in probe
    finally:
        await second.aclose()


@pytest.mark.asyncio
async def test_creating_without_pid_recovers_exact_identity_then_remounts():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    candidate = manager._state_for_mount(_cloud_mount_cfg()["mounts"][0], 0)
    old_generation = "a" * 32
    old_pass = "oldResidentPass_1234567890"
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_HEAL__\t"
        f"{candidate.resident_spec_digest}\t{old_generation}\t0\t{old_pass}\n"
    )

    await manager.start_all()
    try:
        unmount = next(
            body
            for path, body in backend.files.items()
            if path.endswith("unmount_srw-thread-1-home.sh")
        )
        assert f'[ "$_srw_generation" = {old_generation} ]' in unmount
        assert old_pass in unmount
        mounted = manager.mounts[0]
        assert mounted.resident_generation != old_generation
        mount_script = next(
            body
            for path, body in backend.files.items()
            if Path(path).name == "mount_srw-thread-1-home.sh"
        )
        assert "_srw_write_identity creating 0" in mount_script
        assert "_srw_write_identity active" in mount_script
    finally:
        await manager.aclose()


def test_resident_spec_excludes_secret_and_basic_rotation_forces_probe_check():
    old_cfg = _cloud_mount_cfg()
    new_cfg = copy.deepcopy(old_cfg)
    new_cfg["mounts"][0]["auth"]["password"] = "rotated-super-secret"
    backend = FakeFencedRemoteBackend()
    old_manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=old_cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    new_manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=new_cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    old_state = old_manager._state_for_mount(old_cfg["mounts"][0], 0)
    new_state = new_manager._state_for_mount(new_cfg["mounts"][0], 0)

    assert old_state.resident_spec_digest == new_state.resident_spec_digest
    assert hashlib.sha256(b"rotated-super-secret").hexdigest() not in (
        new_state.resident_spec_digest
    )
    probe = new_manager._resident_probe_script(new_cfg["mounts"][0], new_state)
    assert "rclone reveal" in probe
    assert "rotated-super-secret" not in probe
    assert "base64 -d" in probe


def test_unknown_secret_config_is_not_hashed_and_forces_conservative_remount():
    cfg_one = _cloud_mount_cfg()
    cfg_two = copy.deepcopy(cfg_one)
    cfg_one["mounts"][0]["source"]["config"]["api_key"] = "first-secret"
    cfg_two["mounts"][0]["source"]["config"]["api_key"] = "second-secret"
    manager_one = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg_one,
        workspace_backend=FakeFencedRemoteBackend(),
        workspace_root=Path("/workspace"),
    )
    manager_two = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg_two,
        workspace_backend=FakeFencedRemoteBackend(),
        workspace_root=Path("/workspace"),
    )
    state_one = manager_one._state_for_mount(cfg_one["mounts"][0], 0)
    state_two = manager_two._state_for_mount(cfg_two["mounts"][0], 0)

    assert state_one.resident_spec_digest == state_two.resident_spec_digest
    assert hashlib.sha256(b"first-secret").hexdigest() != state_one.resident_spec_digest
    assert manager_one._resident_adoption_safe(cfg_one["mounts"][0]) is False
    assert manager_two._resident_adoption_safe(cfg_two["mounts"][0]) is False


def test_effective_default_ignore_change_rotates_non_secret_spec(monkeypatch):
    monkeypatch.delenv("SRW_CLOUD_MOUNT_DEFAULT_IGNORES", raising=False)
    cfg_one = _cloud_mount_cfg()
    cfg_two = copy.deepcopy(cfg_one)
    cfg_one["default_ignores"] = ["one/**"]
    cfg_two["default_ignores"] = ["two/**"]
    one = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg_one,
        workspace_backend=FakeFencedRemoteBackend(),
        workspace_root=Path("/workspace"),
    )
    two = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg_two,
        workspace_backend=FakeFencedRemoteBackend(),
        workspace_root=Path("/workspace"),
    )

    assert (
        one._state_for_mount(cfg_one["mounts"][0], 0).resident_spec_digest
        != two._state_for_mount(cfg_two["mounts"][0], 0).resident_spec_digest
    )


@pytest.mark.asyncio
async def test_fresh_bearer_publication_precedes_bounded_rc_refresh(
    fake_token_client,
):
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_opencloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )
    candidate = manager._state_for_mount(_opencloud_mount_cfg()["mounts"][0], 0)
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_ADOPTED__\t"
        f"{candidate.resident_spec_digest}\t{'b' * 32}\t321\t"
        "residentPass_123456789012345\n"
    )

    await manager.start_all()
    try:
        publish_index = backend.resource_operations.index(
            "publish bearer token for legacy-session"
        )
        probe_index = backend.resource_operations.index(
            "run rclone script probe_srw-thread-1-home.sh"
        )
        assert publish_index < probe_index
        probe = next(
            body
            for path, body in backend.files.items()
            if path.endswith("probe_srw-thread-1-home.sh")
        )
        assert "vfs/refresh recursive=false" in probe
        assert "recursive=true" not in probe
        assert probe.index("vfs/refresh recursive=false") < probe.index(
            "timeout 15 find"
        )
    finally:
        await manager.aclose()


@pytest.mark.asyncio
async def test_adopted_mount_is_not_rolled_back_when_later_mount_fails():
    cfg = _cloud_mount_cfg()
    second_mount = copy.deepcopy(cfg["mounts"][0])
    second_mount.update(
        {
            "mount_id": "second-mount",
            "workspace_name": "second",
            "target_path": "/cloud/second",
        }
    )
    cfg["mounts"].append(second_mount)
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )
    first_state = manager._state_for_mount(cfg["mounts"][0], 0)
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_ADOPTED__\t"
        f"{first_state.resident_spec_digest}\t{'c' * 32}\t321\t"
        "residentPass_123456789012345\n"
    )
    backend.outputs_by_script["probe_srw-thread-1-second.sh"] = (
        "__SRW_RCLONE_RESIDENT_HEAL__\n"
    )
    backend.outputs_by_script["mount_srw-thread-1-second.sh"] = (
        "__SRW_RCLONE_MOUNT_FAILED__ rc=1\n"
    )

    with pytest.raises(RcloneMountError):
        await manager.start_all()

    assert not any(
        operation.endswith("unmount_srw-thread-1-home.sh")
        for operation in backend.resource_operations
    )
    assert (
        sum(
            operation.endswith("unmount_srw-thread-1-second.sh")
            for operation in backend.resource_operations
        )
        == 2
    )


@pytest.mark.asyncio
async def test_adopted_mount_is_not_rolled_back_when_link_install_fails():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )
    candidate = manager._state_for_mount(_cloud_mount_cfg()["mounts"][0], 0)
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_ADOPTED__\t"
        f"{candidate.resident_spec_digest}\t{'d' * 32}\t321\t"
        "residentPass_123456789012345\n"
    )
    backend.outputs_by_script["install_cloud_links.sh"] = (
        "__SRW_RCLONE_MOUNT_FAILED__ rc=1\n"
    )

    with pytest.raises(RcloneMountError):
        await manager.start_all()

    assert not any("unmount_" in operation for operation in backend.resource_operations)


@pytest.mark.asyncio
async def test_failed_new_generation_rolls_back_with_its_exact_identity():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_HEAL__\n"
    )
    backend.outputs_by_script["mount_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_MOUNT_FAILED__ rc=1\n"
    )
    with (
        patch(
            "shared.runtime.services.cloud_mount.uuid.uuid4",
            side_effect=[
                SimpleNamespace(hex="1" * 32),
                SimpleNamespace(hex="2" * 32),
            ],
        ),
        patch(
            "shared.runtime.services.cloud_mount.secrets.token_urlsafe",
            side_effect=["candidatePass_123456789012", "newPass_123456789012345678"],
        ),
    ):
        with pytest.raises(RcloneMountError):
            await manager.start_all()

    rollback = next(
        body
        for path, body in backend.files.items()
        if path.endswith("unmount_srw-thread-1-home.sh")
    )
    assert "2" * 32 in rollback
    assert "newPass_123456789012345678" in rollback
    assert "candidatePass_123456789012" not in rollback


@pytest.mark.asyncio
async def test_stateless_failed_mount_reports_clean_only_after_strict_rollback():
    backend = FakeFencedRemoteBackend()
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_HEAL__\n"
    )
    backend.outputs_by_script["mount_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_MOUNT_FAILED__ rc=124\n"
    )
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )

    with pytest.raises(RcloneMountCleanFailure):
        await manager.start_all()

    assert any(
        operation.endswith("unmount_srw-thread-1-home.sh")
        for operation in backend.resource_operations
    )


@pytest.mark.asyncio
async def test_stateless_failed_rollback_never_reports_clean_failure():
    backend = FakeFencedRemoteBackend()
    backend.outputs_by_script["probe_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_RESIDENT_HEAL__\n"
    )
    backend.outputs_by_script["mount_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_MOUNT_FAILED__ rc=124\n"
    )
    backend.outputs_by_script["unmount_srw-thread-1-home.sh"] = (
        "__SRW_RCLONE_MOUNT_FAILED__ rc=1\n"
    )
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )

    with pytest.raises(RcloneMountError) as exc_info:
        await manager.start_all()

    assert not isinstance(exc_info.value, RcloneMountCleanFailure)


def test_pinned_mount_script_has_no_resident_identity_protocol():
    backend = FakeRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )
    state = manager._state_for_mount(_cloud_mount_cfg()["mounts"][0], 0)

    script = manager._mount_script(_cloud_mount_cfg()["mounts"][0], state)

    assert "MOUNT_ARGS=(nohup rclone" in script
    assert "resident.identity" not in script
    assert "_srw_write_identity" not in script


def test_script_staging_paths_are_controller_unique():
    one = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=FakeRemoteBackend(),
        workspace_root=Path("/workspace"),
    )
    two = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=FakeRemoteBackend(),
        workspace_root=Path("/workspace"),
    )
    one._run_remote_script("probe.sh", "echo __SRW_RCLONE_MOUNT_OK__")
    two._run_remote_script("probe.sh", "echo __SRW_RCLONE_MOUNT_OK__")

    assert one._script_nonce != two._script_nonce
    assert any(
        f"/scripts/{one._script_nonce}/" in path for path in one.workspace_backend.files
    )
    assert any(
        f"/scripts/{two._script_nonce}/" in path for path in two.workspace_backend.files
    )


def test_staged_script_has_remote_kill_budget_before_ssh_timeout():
    backend = FakeFencedRemoteBackend()
    manager = RcloneMountManager(
        thread_id="thread-12345678",
        cloud_cfg=_cloud_mount_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/workspace"),
    )

    manager._run_remote_script(
        "bounded.sh",
        "echo __SRW_RCLONE_MOUNT_OK__",
        timeout=17,
    )

    command, ssh_timeout = backend.commands[-1]
    assert "timeout --signal=TERM --kill-after=5s 17s bash" in command
    assert ssh_timeout == 57


def test_a_group_folder_mount_stays_adoptable():
    """``nextcloud_chunk_size`` is not a secret: a Group Folder mount (which
    sets it to 0) may still adopt its resident rclone."""
    cfg = _cloud_mount_cfg()
    mount = cfg["mounts"][0]
    mount["source"]["config"]["nextcloud_chunk_size"] = "0"
    assert RcloneMountManager._resident_adoption_safe(mount) is True
    mount["source"]["config"]["bearer_token"] = "t"
    assert RcloneMountManager._resident_adoption_safe(mount) is False
