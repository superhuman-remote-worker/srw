from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from shared.runtime.services.cloud_mount import RcloneMountManager
from shared.runtime.services.cloud_overlay.overlay_mount import (
    OverlayMountError,
    OverlayMountManager,
)


class FakeRemoteBackend:
    def __init__(self, *, root: str | None = None) -> None:
        self.files: dict[str, str] = {}
        self.commands: list[tuple[str, int]] = []
        self.outputs_by_script: dict[str, str] = {}
        if root is not None:
            self.root = root

    def resolve_home_path(self, relative_path: str) -> str:
        return f"/home/agent-host/{relative_path}"

    def write_home_file(self, relative_path: str, content) -> None:
        self.files[relative_path] = (
            content.decode("utf-8") if isinstance(content, bytes) else content
        )

    def exec_command(self, command: str, timeout: int = 30) -> str:
        self.commands.append((command, timeout))
        for name, out in self.outputs_by_script.items():
            if name in command:
                return out
        if "overlay_adopt_probe.sh" in command:
            return "__SRW_OVERLAY_ABSENT__\n"
        return "__SRW_OVERLAY_OK__\n"


class SharedOverlayRuntime:
    """Workspace-side state shared by two claim-scoped backend instances."""

    def __init__(self) -> None:
        self.current_token = 1
        self.overlay_state = "absent"
        self.upper_bytes = b""
        self.work_epoch = 0
        self.files: dict[str, str] = {}
        self.operations: list[tuple[int, str]] = []


class ClaimFencedBackend:
    def __init__(self, runtime: SharedOverlayRuntime, token: int) -> None:
        self.runtime = runtime
        self.token = token

    def resolve_home_path(self, relative_path: str) -> str:
        return f"/home/agent-host/{relative_path}"

    def write_home_file(self, relative_path: str, content) -> None:
        self.runtime.files[relative_path] = (
            content.decode("utf-8") if isinstance(content, bytes) else content
        )

    @property
    def claim_resource_fenced(self) -> bool:
        return True

    def exec_command(self, command: str, timeout: int = 30) -> str:
        raise AssertionError("stateless overlay commands must use the claim fence")

    def exec_claim_resource(
        self,
        command: str,
        timeout: int = 30,
        *,
        operation: str,
    ) -> str:
        del timeout
        if self.token != self.runtime.current_token:
            raise RuntimeError("stale claim-resource owner")
        self.runtime.operations.append((self.token, operation))
        if "overlay_adopt_probe.sh" in command:
            if self.runtime.overlay_state == "healthy":
                return "__SRW_OVERLAY_ADOPTED__\n__SRW_OVERLAY_OK__\n"
            if self.runtime.overlay_state == "creating-healthy":
                self.runtime.overlay_state = "healthy"
                return "__SRW_OVERLAY_ADOPTED__\n__SRW_OVERLAY_OK__\n"
            if self.runtime.overlay_state == "dead":
                return "__SRW_OVERLAY_DEAD__ ENOTCONN\n"
            if self.runtime.overlay_state == "mismatch":
                return "__SRW_OVERLAY_MISMATCH__\n"
            return "__SRW_OVERLAY_ABSENT__\n"
        if "overlay_mount.sh" in command:
            self.runtime.overlay_state = "healthy"
        elif "overlay_heal_unmount.sh" in command:
            self.runtime.overlay_state = "absent"
        elif "overlay_remount.sh" in command:
            self.runtime.work_epoch += 1
            self.runtime.overlay_state = "healthy"
        elif "overlay_unmount.sh" in command:
            self.runtime.overlay_state = "absent"
        elif "overlay_probe.sh" in command:
            if self.runtime.overlay_state != "healthy":
                return "__SRW_OVERLAY_DEAD__ ENOTCONN\n"
        return "__SRW_OVERLAY_OK__\n"


def _cfg() -> dict:
    return {
        "lower": "/cloud/lower",
        "upper": "/home/agent-host/.overlay/upper",
        "work": "/home/agent-host/.overlay/work",
        "merged": "/cloud/merged",
        "quota_bytes": 8 * 1024**3,
    }


def _manager(backend) -> OverlayMountManager:
    return OverlayMountManager(
        thread_id="thread-12345678",
        overlay_cfg=_cfg(),
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )


def test_generated_overlay_scripts_are_valid_bash():
    mgr = _manager(FakeRemoteBackend())
    scripts = {
        "adopt": mgr._adopt_probe_script(),
        "stateless-cold": mgr._mount_script(),
        "pinned-mount": mgr._mount_script(replace_existing=True),
        "unmount": mgr._unmount_script(),
        "plain-unmount": mgr._plain_unmount_script(),
        "remount": mgr._mount_body_only_script(),
        "probe": mgr._probe_script(),
        "heal-unmount": mgr._heal_unmount_script(),
        "reset-unmount": mgr._reset_unmount_script(),
        "wipe-upper": mgr._wipe_upper_script(),
        "usage": mgr._usage_script(),
    }
    for name, script in scripts.items():
        result = subprocess.run(
            ["bash", "-n"], input=script, text=True, capture_output=True, check=False
        )
        assert result.returncode == 0, f"{name}: {result.stderr}"


def test_mount_script_builds_fuse_overlayfs_over_ro_lower_and_repoints_symlink():
    backend = FakeRemoteBackend()
    _manager(backend).mount()
    scripts = [b for p, b in backend.files.items() if p.endswith("overlay_mount.sh")]
    assert len(scripts) == 1
    s = scripts[0]
    # exact spike option string, no extra overlay opts (Global Constraints)
    assert (
        "fuse-overlayfs -o "
        "lowerdir=/cloud/lower,upperdir=/home/agent-host/.overlay/upper,"
        "workdir=/home/agent-host/.overlay/work /cloud/merged" in s
    )
    assert "mkdir -p /home/agent-host/.overlay/upper" in s
    assert "mountpoint -q /cloud/lower" in s  # refuses if the lower isn't up
    assert "fusermount3 -u /cloud/merged" in s  # pinned path remains idempotent
    assert not any(path.endswith("overlay_adopt_probe.sh") for path in backend.files)
    # symlink workspace/cloud -> merged (NOT the raw lower)
    assert "ln -sfn /cloud/merged" in s
    assert "${workspace}/cloud" in s


def test_mount_refuses_when_lower_not_mounted():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_mount.sh"] = "__SRW_OVERLAY_FAILED__ rc=1\n"
    with pytest.raises(OverlayMountError):
        _manager(backend).mount()


def test_unmount_is_plain_and_leaves_upperdir():
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    mgr.unmount()
    scripts = [b for p, b in backend.files.items() if p.endswith("overlay_unmount.sh")]
    assert len(scripts) == 1
    u = scripts[0]
    assert "fusermount3 -u /cloud/merged" in u
    # unmount must NOT rm the upperdir/workdir
    assert "rm -rf /home/agent-host/.overlay/upper" not in u
    assert "rm -rf /home/agent-host/.overlay/work" not in u


def test_strict_unmount_failure_reports_the_fusermount_error(tmp_path):
    # An unprivileged fusermount3 refuses to unmount without /etc/mtab, which
    # containerd doesn't add (dev, 10-19); the refusal belongs in the report.
    stubs = {
        "mountpoint": "exit 0",
        "fusermount3": (
            "echo 'fusermount3: failed to open /etc/mtab: "
            "No such file or directory' >&2; exit 1"
        ),
        "fusermount": "exit 1",
    }
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, body in stubs.items():
        stub = bin_dir / name
        stub.write_text(f"#!/bin/sh\n{body}\n")
        stub.chmod(0o755)
    script = _manager(FakeRemoteBackend())._unmount_script(strict=True)

    result = subprocess.run(
        ["bash"],
        input=script,
        text=True,
        capture_output=True,
        check=False,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin"},
    )

    assert result.returncode == 6
    assert "__SRW_OVERLAY_FAILED__ rc=6" in result.stdout
    assert "failed to open /etc/mtab" in result.stdout


def test_mount_script_quotes_paths_with_spaces():
    """Verify that paths with spaces are properly quoted in fuse-overlayfs -o option."""
    backend = FakeRemoteBackend()
    # Config with a space in the lower path
    cfg = _cfg()
    cfg["lower"] = "/cloud/low er"
    mgr = OverlayMountManager(
        thread_id="thread-12345678",
        overlay_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    mgr.mount()
    scripts = [b for p, b in backend.files.items() if p.endswith("overlay_mount.sh")]
    assert len(scripts) == 1
    s = scripts[0]
    # The -o value should be quoted as ONE shell word, preserving the space
    assert (
        "'lowerdir=/cloud/low er,upperdir=/home/agent-host/.overlay/upper,workdir=/home/agent-host/.overlay/work'"
        in s
    )
    # The unquoted form must NOT appear
    assert "-o lowerdir=/cloud/low er," not in s


def test_refresh_unmounts_overlay_refreshes_lower_then_remounts():
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    starting_commands = len(backend.commands)
    calls: list[int] = []

    def _refresh_lower() -> None:
        # Record how many remote scripts have run since mount(): must be
        # exactly 1 (the plain unmount) when this callback fires, proving
        # order: plain-unmount -> callback -> remount.
        calls.append(len(backend.commands) - starting_commands)

    mgr.refresh(_refresh_lower)
    assert calls == [1]

    commands = [cmd for cmd, _timeout in backend.commands[starting_commands:]]
    assert len(commands) == 2, (
        "expected exactly 2 remote scripts: pre-refresh unmount + remount"
    )
    assert "overlay_pre_refresh_unmount.sh" in commands[0]
    assert "overlay_remount.sh" in commands[1]

    unmount_script = next(
        b
        for p, b in backend.files.items()
        if p.endswith("overlay_pre_refresh_unmount.sh")
    )
    assert "fusermount3 -u /cloud/merged" in unmount_script  # PLAIN unmount (not -uz)
    assert "-uz" not in unmount_script

    remount_script = next(
        b for p, b in backend.files.items() if p.endswith("overlay_remount.sh")
    )
    assert "fuse-overlayfs -o lowerdir=/cloud/lower" in remount_script


def test_health_check_true_on_ok_false_on_enotconn():
    ok_backend = FakeRemoteBackend()
    assert _manager(ok_backend).health_check() is True

    dead = FakeRemoteBackend()
    dead.outputs_by_script["overlay_probe.sh"] = "__SRW_OVERLAY_DEAD__ ENOTCONN\n"
    assert _manager(dead).health_check() is False


def test_health_check_false_when_no_sentinel_in_output():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_probe.sh"] = "garbled output with no sentinel\n"
    assert _manager(backend).health_check() is False


def test_successor_adopts_shared_healthy_overlay_without_unmounting_upper():
    runtime = SharedOverlayRuntime()
    first = _manager(ClaimFencedBackend(runtime, token=1))

    assert first.mount() == "cold"
    cold_script = next(
        body
        for path, body in runtime.files.items()
        if path.endswith("overlay_mount.sh")
    )
    assert "resident.identity" in cold_script
    assert first._identity_digest in cold_script
    runtime.upper_bytes = b"unapplied staged bytes"
    first.detach_local()
    assert first.active is False
    assert runtime.overlay_state == "healthy"

    runtime.current_token = 2
    successor = _manager(ClaimFencedBackend(runtime, token=2))
    before = len(runtime.operations)
    assert successor.mount() == "adopted"

    successor_ops = [op for _token, op in runtime.operations[before:]]
    assert successor_ops == ["cloud overlay overlay_adopt_probe"]
    assert runtime.upper_bytes == b"unapplied staged bytes"
    assert runtime.work_epoch == 0
    assert runtime.overlay_state == "healthy"


def test_stateless_adoption_fails_closed_on_exact_overlay_config_change():
    runtime = SharedOverlayRuntime()
    first_backend = ClaimFencedBackend(runtime, token=1)
    first = _manager(first_backend)
    assert first.mount() == "cold"
    runtime.upper_bytes = b"belongs to the original overlay layout"

    changed_cfg = _cfg()
    changed_cfg["upper"] = "/home/agent-host/.overlay/other-upper"
    changed = OverlayMountManager(
        thread_id="thread-12345678",
        overlay_cfg=changed_cfg,
        workspace_backend=ClaimFencedBackend(runtime, token=2),
        workspace_root=Path("/home/agent-host/workspace"),
    )
    assert changed._identity_digest != first._identity_digest
    adopt_script = changed._adopt_probe_script()
    assert changed._identity_digest in adopt_script
    assert "__SRW_OVERLAY_MISMATCH__" in adopt_script

    runtime.current_token = 2
    runtime.overlay_state = "mismatch"
    before = len(runtime.operations)
    with pytest.raises(OverlayMountError, match="identity does not match"):
        changed.mount(lambda: pytest.fail("mismatch must not restart the lower"))

    assert [op for _token, op in runtime.operations[before:]] == [
        "cloud overlay overlay_adopt_probe"
    ]
    # Attach rollback owns no resident resource on mismatch.  It must remain
    # local-only and must not execute even an identity-fenced unmount script.
    assert changed.rollback_failed_mount() is False
    assert [op for _token, op in runtime.operations[before:]] == [
        "cloud overlay overlay_adopt_probe"
    ]
    assert runtime.upper_bytes == b"belongs to the original overlay layout"
    assert runtime.work_epoch == 0


def test_stateless_cold_partial_mount_runs_exact_identity_rollback():
    class PartialMountBackend(ClaimFencedBackend):
        def exec_claim_resource(
            self, command: str, timeout: int = 30, *, operation: str
        ) -> str:
            if "overlay_mount.sh" in command:
                self.runtime.operations.append((self.token, operation))
                self.runtime.overlay_state = "healthy"
                return "__SRW_OVERLAY_FAILED__ rc=71\n"
            if "overlay_failed_mount_rollback.sh" in command:
                self.runtime.operations.append((self.token, operation))
                self.runtime.overlay_state = "absent"
                return "__SRW_OVERLAY_OK__\n"
            return super().exec_claim_resource(
                command, timeout=timeout, operation=operation
            )

    runtime = SharedOverlayRuntime()
    backend = PartialMountBackend(runtime, token=1)
    manager = _manager(backend)

    with pytest.raises(OverlayMountError, match="did not report OK"):
        manager.mount()

    assert runtime.overlay_state == "healthy"
    assert manager.rollback_failed_mount() is True
    assert runtime.overlay_state == "absent"
    assert [op for _token, op in runtime.operations] == [
        "cloud overlay overlay_adopt_probe",
        "cloud overlay overlay_mount",
        "cloud overlay overlay_failed_mount_rollback",
    ]
    rollback = next(
        body
        for path, body in runtime.files.items()
        if path.endswith("overlay_failed_mount_rollback.sh")
    )
    assert manager._identity_value("creating") in rollback
    assert manager._identity_value("active") in rollback
    assert "rm -rf -- /home/agent-host/.overlay/upper" not in rollback
    assert "rm -rf -- /home/agent-host/.overlay/work" not in rollback


def test_successor_converges_crash_after_mount_before_active_publication():
    runtime = SharedOverlayRuntime()
    predecessor = _manager(ClaimFencedBackend(runtime, token=1))
    cold_script = predecessor._mount_script()
    creating = predecessor._identity_value("creating")
    active = predecessor._identity_value("active")
    fuse_index = cold_script.index("fuse-overlayfs -o")
    assert cold_script.index(creating) < fuse_index
    assert cold_script.rindex(active) > fuse_index

    # Fault boundary: fuse succeeded and upper bytes exist, but the process
    # died before replacing `creating` with `active`.
    runtime.overlay_state = "creating-healthy"
    runtime.upper_bytes = b"bytes captured before predecessor crash"
    runtime.current_token = 2
    successor = _manager(ClaimFencedBackend(runtime, token=2))

    assert successor.mount() == "adopted"
    assert runtime.overlay_state == "healthy"
    assert runtime.upper_bytes == b"bytes captured before predecessor crash"
    assert runtime.work_epoch == 0
    adopt_script = successor._adopt_probe_script()
    assert creating in adopt_script
    assert active in adopt_script


def test_successor_recreates_only_work_after_crash_between_unmount_and_remount():
    runtime = SharedOverlayRuntime()
    # Fault boundary: heal/refresh unmounted the merged view, then its agent
    # died before the fresh-work remount. The staged upper must survive while
    # the single-mount workdir is recreated by the successor's ABSENT path.
    runtime.overlay_state = "absent"
    runtime.upper_bytes = b"staged bytes captured before unmount"
    runtime.current_token = 2
    successor = _manager(ClaimFencedBackend(runtime, token=2))

    assert successor.mount() == "cold"
    assert runtime.overlay_state == "healthy"
    assert runtime.upper_bytes == b"staged bytes captured before unmount"

    cold_script = next(
        body
        for path, body in runtime.files.items()
        if path.endswith("overlay_mount.sh")
    )
    guard_index = cold_script.index("overlay appeared during cold mount")
    work_reset_index = cold_script.index("rm -rf -- /home/agent-host/.overlay/work")
    mount_index = cold_script.index("fuse-overlayfs -o")
    assert guard_index < work_reset_index < mount_index
    assert "mkdir -p -- /home/agent-host/.overlay/work" in cold_script
    assert "rm -rf -- /home/agent-host/.overlay/upper" not in cold_script

    # Pinned replacement remains byte-for-byte in lifecycle semantics: its
    # historical path does not gain the stateless recovery wipe.
    pinned_script = successor._mount_script(replace_existing=True)
    assert "rm -rf -- /home/agent-host/.overlay/work" not in pinned_script


def test_successor_heals_dead_overlay_once_with_fresh_work_and_preserved_upper():
    runtime = SharedOverlayRuntime()
    runtime.current_token = 2
    runtime.overlay_state = "dead"
    runtime.upper_bytes = b"staged before lower died"
    backend = ClaimFencedBackend(runtime, token=2)
    successor = _manager(backend)
    lower_restarts = 0

    def _restart_exact_lower() -> None:
        nonlocal lower_restarts
        lower_restarts += 1

    assert successor.mount(_restart_exact_lower) == "healed"
    assert lower_restarts == 1
    assert runtime.work_epoch == 1
    assert runtime.upper_bytes == b"staged before lower died"
    assert runtime.overlay_state == "healthy"

    remount_script = next(
        body
        for path, body in runtime.files.items()
        if path.endswith("overlay_remount.sh")
    )
    assert "rm -rf /home/agent-host/.overlay/work" in remount_script
    assert "rm -rf /home/agent-host/.overlay/upper" not in remount_script


def test_stale_predecessor_probe_and_heal_are_rejected_after_handoff():
    runtime = SharedOverlayRuntime()
    predecessor = _manager(ClaimFencedBackend(runtime, token=1))
    assert predecessor.mount() == "cold"
    runtime.upper_bytes = b"successor-owned staged bytes"

    runtime.current_token = 2
    successor = _manager(ClaimFencedBackend(runtime, token=2))
    assert successor.mount() == "adopted"
    operations_before_stale_calls = list(runtime.operations)
    stale_callback_calls = 0

    def _stale_restart() -> None:
        nonlocal stale_callback_calls
        stale_callback_calls += 1

    with pytest.raises(RuntimeError, match="stale claim-resource owner"):
        predecessor.health_check()
    with pytest.raises(RuntimeError, match="stale claim-resource owner"):
        predecessor.heal(_stale_restart)

    assert runtime.operations == operations_before_stale_calls
    assert stale_callback_calls == 0
    assert runtime.upper_bytes == b"successor-owned staged bytes"
    assert runtime.overlay_state == "healthy"


def test_terminal_unmount_remains_remote_but_handoff_detach_is_local_only():
    runtime = SharedOverlayRuntime()
    manager = _manager(ClaimFencedBackend(runtime, token=1))
    assert manager.mount() == "cold"
    operation_count = len(runtime.operations)

    manager.detach_local()
    assert len(runtime.operations) == operation_count
    assert runtime.overlay_state == "healthy"

    # Terminal cleanup happens while the same claim still owns the fence.
    manager.unmount()
    assert runtime.operations[-1] == (1, "cloud overlay overlay_unmount")
    assert runtime.overlay_state == "absent"


def test_heal_lazy_unmounts_overlay_first_then_remounts_lower_then_overlay():
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    starting_commands = len(backend.commands)
    calls: list[int] = []

    def _remount_lower() -> None:
        # Record how many remote scripts have run since mount(): must be
        # exactly 1 (the lazy heal-unmount) when this callback fires, proving
        # order: lazy-unmount -> callback -> remount.
        calls.append(len(backend.commands) - starting_commands)

    mgr.heal(_remount_lower)
    assert calls == [1], "callback must fire after exactly 1 script (heal-unmount)"

    commands = [cmd for cmd, _timeout in backend.commands[starting_commands:]]
    assert len(commands) == 2, (
        "expected exactly 2 remote scripts: heal-unmount + remount"
    )
    assert "overlay_heal_unmount.sh" in commands[0]
    assert "overlay_remount.sh" in commands[1]

    unmount = next(
        b for p, b in backend.files.items() if p.endswith("overlay_heal_unmount.sh")
    )
    assert "fusermount3 -uz /cloud/merged" in unmount  # LAZY is correct on heal
    assert "dead overlay remained mounted" in unmount

    remount = next(
        b for p, b in backend.files.items() if p.endswith("overlay_remount.sh")
    )
    assert "fuse-overlayfs -o lowerdir=/cloud/lower" in remount
    assert "fusermount3 -u" not in remount  # no unmount folded in


def test_mount_body_script_freshens_workdir_not_upper():
    """Task 11: heal/refresh remounts must wipe+recreate the workdir (a
    fuse-overlayfs workdir must never be reused across mounts) but MUST NOT
    touch the upperdir — staged data has to survive a heal (design §11.2/§11.6)."""
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    starting_commands = len(backend.commands)

    calls: list[int] = []

    def _remount_lower() -> None:
        calls.append(len(backend.commands) - starting_commands)

    mgr.heal(_remount_lower)
    assert calls == [1]

    remount_script = next(
        b for p, b in backend.files.items() if p.endswith("overlay_remount.sh")
    )
    assert "rm -rf /home/agent-host/.overlay/work" in remount_script
    assert "mkdir -p /home/agent-host/.overlay/work" in remount_script
    # the rm must land BEFORE the fuse-overlayfs mount line
    rm_idx = remount_script.index("rm -rf /home/agent-host/.overlay/work")
    mount_idx = remount_script.index("fuse-overlayfs -o")
    assert rm_idx < mount_idx
    # upperdir must NEVER be wiped by this script
    assert "rm -rf /home/agent-host/.overlay/upper" not in remount_script


def test_upperdir_usage_parses_du_and_flags_over_quota():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_usage.sh"] = (
        "9663676416\t/home/agent-host/.overlay/upper\n__SRW_OVERLAY_OK__\n"
    )
    mgr = _manager(backend)  # quota_bytes = 8 GiB
    assert mgr.upperdir_usage_bytes() == 9663676416
    assert mgr.over_quota() is True
    assert mgr.quota_guard_message() is not None


def test_under_quota_has_no_guard_message():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_usage.sh"] = (
        "1024\t/home/agent-host/.overlay/upper\n__SRW_OVERLAY_OK__\n"
    )
    mgr = _manager(backend)
    assert mgr.over_quota() is False
    assert mgr.quota_guard_message() is None


def test_upperdir_usage_skips_sentinel_and_junk_lines_falls_back_to_zero():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_usage.sh"] = (
        "du: cannot access: No such file\n__SRW_OVERLAY_OK__\n"
    )
    mgr = _manager(backend)
    assert mgr.upperdir_usage_bytes() == 0
    assert mgr.over_quota() is False


def test_over_quota_false_when_quota_bytes_absent():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_usage.sh"] = (
        "999999999999\t/home/agent-host/.overlay/upper\n__SRW_OVERLAY_OK__\n"
    )
    cfg = _cfg()
    del cfg["quota_bytes"]
    mgr = OverlayMountManager(
        thread_id="thread-12345678",
        overlay_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    assert mgr.over_quota() is False
    assert mgr.quota_guard_message() is None


def test_over_quota_true_at_exact_quota_boundary():
    """used == quota_bytes must block AT the cap, not only strictly above it."""
    backend = FakeRemoteBackend()
    quota = 8 * 1024**3  # matches _cfg()'s quota_bytes
    backend.outputs_by_script["overlay_usage.sh"] = (
        f"{quota}\t/home/agent-host/.overlay/upper\n__SRW_OVERLAY_OK__\n"
    )
    mgr = _manager(backend)
    assert mgr.over_quota() is True
    assert mgr.quota_guard_message() is not None


def test_over_quota_false_when_quota_bytes_zero():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_usage.sh"] = (
        "999999999999\t/home/agent-host/.overlay/upper\n__SRW_OVERLAY_OK__\n"
    )
    cfg = _cfg()
    cfg["quota_bytes"] = 0
    mgr = OverlayMountManager(
        thread_id="thread-12345678",
        overlay_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    assert mgr.over_quota() is False
    assert mgr.quota_guard_message() is None


def test_protected_payload_mounts_lower_then_overlay():
    """B9 seam test: a protected _build_protected_cloud_mount-shaped payload
    drives RcloneMountManager (RO lower, skip_workspace_links) and then
    OverlayMountManager (capture overlay + workspace/cloud -> merged symlink)
    against ONE FakeRemoteBackend — mirrors what persistent_session's
    _setup_cloud_mount does when it stacks the two managers.
    """
    backend = FakeRemoteBackend()
    # This fixture's default exec_command output satisfies OverlayMountManager's
    # sentinel (__SRW_OVERLAY_OK__); RcloneMountManager checks a different one
    # (__SRW_RCLONE_MOUNT_OK__), so pin the rclone mount script's output too.
    backend.outputs_by_script["mount_srw-thread-t-lower.sh"] = (
        "__SRW_RCLONE_MOUNT_OK__\n"
    )
    cfg = {  # shape of _build_protected_cloud_mount output
        "driver": "rclone",
        "protected": True,
        "skip_workspace_links": True,
        "overlay": {
            "lower": "/cloud/lower",
            "upper": "/home/agent-host/.overlay/upper",
            "work": "/home/agent-host/.overlay/work",
            "merged": "/cloud/merged",
            "quota_bytes": 8 * 1024**3,
        },
        "mounts": [
            {
                "mount_id": "protected-t",
                "mount_kind": "protected_lower",
                "target_path": "/cloud/lower",
                "workspace_name": "lower",
                "access": "read_only",
                "backend": "nextcloud",
                "source": {
                    "type": "webdav",
                    "config": {
                        "url": "https://nc/x/",
                        "vendor": "nextcloud",
                        "user": "srw-reader-u",
                    },
                },
                "auth": {"type": "basic", "password": "p"},
            }
        ],
    }

    rclone_mgr = RcloneMountManager(
        thread_id="thread-t",
        cloud_cfg=cfg,
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    rclone_mgr._start_all_sync()
    assert rclone_mgr.active is True
    # overlay owns the symlink in protected mode -> rclone must not install one
    assert not any(p.endswith("install_cloud_links.sh") for p in backend.files)

    overlay_mgr = OverlayMountManager(
        thread_id="thread-t",
        overlay_cfg=cfg["overlay"],
        workspace_backend=backend,
        workspace_root=Path("/home/agent-host/workspace"),
    )
    overlay_mgr.mount()
    assert overlay_mgr.active is True
    assert any(p.endswith("overlay_mount.sh") for p in backend.files)
