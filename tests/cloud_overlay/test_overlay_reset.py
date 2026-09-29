from __future__ import annotations

from pathlib import Path
import re
from unittest.mock import MagicMock

import pytest

from shared.runtime.services.cloud_overlay.overlay_mount import (
    OverlayMountError,
    OverlayMountManager,
)


_RESET_THREAD_ID = "thread-12345678"
_RESET_AGENT_ID = "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
_RESET_RUNTIME_GENERATION = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
_RESET_ATTACH_TOKEN = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
_RESET_WORKSPACE_GENERATION = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
_RESET_RUNTIME_INCARNATION = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def _reset_headers() -> dict[str, str]:
    return {
        "X-Agent-ID": _RESET_AGENT_ID,
        "X-Session-Runtime-Generation": _RESET_RUNTIME_GENERATION,
        "X-Session-Runtime-Attach-Token": _RESET_ATTACH_TOKEN,
    }


def _reset_body() -> dict[str, str]:
    return {
        "thread_id": _RESET_THREAD_ID,
        "workspace_generation": _RESET_WORKSPACE_GENERATION,
        "workspace_runtime_incarnation": _RESET_RUNTIME_INCARNATION,
    }


def _assert_sanitized_reset_error(
    body: dict[str, str], *, message: str, leaked_exception_text: str
) -> None:
    assert body["error"] == message
    assert re.fullmatch(r"[0-9a-f]{12}", body["error_ref"])
    assert leaked_exception_text not in str(body)


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


def test_reset_upper_script_order_and_content():
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    starting_commands = len(backend.commands)

    mgr.reset_upper(refresh_lower=lambda: None)

    commands = [cmd for cmd, _timeout in backend.commands[starting_commands:]]
    assert len(commands) == 3, (
        "expected exactly 3 remote scripts: reset-unmount, wipe-upper, remount"
    )
    assert "overlay_reset_unmount.sh" in commands[0]
    assert "overlay_wipe_upper.sh" in commands[1]
    assert "overlay_remount.sh" in commands[2]

    wipe_script = next(
        b for p, b in backend.files.items() if p.endswith("overlay_wipe_upper.sh")
    )
    # rm -rf of BOTH upper and work
    assert (
        "rm -rf /home/agent-host/.overlay/upper /home/agent-host/.overlay/work"
        in wipe_script
    )
    # mkdir -p of both, recreated fresh
    assert (
        "mkdir -p /home/agent-host/.overlay/upper /home/agent-host/.overlay/work"
        in wipe_script
    )
    # NEVER touch the merged mountpoint or the lower
    assert "/cloud/merged" not in wipe_script
    assert "/cloud/lower" not in wipe_script

    remount_script = next(
        b for p, b in backend.files.items() if p.endswith("overlay_remount.sh")
    )
    assert "fuse-overlayfs -o lowerdir=/cloud/lower" in remount_script


def test_reset_upper_unmount_plain_u_with_uz_fallback():
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()

    mgr.reset_upper(refresh_lower=lambda: None)

    unmount_script = next(
        b for p, b in backend.files.items() if p.endswith("overlay_reset_unmount.sh")
    )
    assert "fusermount3 -u /cloud/merged" in unmount_script
    assert "fusermount3 -uz /cloud/merged" in unmount_script
    assert "overlay remained mounted during reset" in unmount_script
    # the plain attempt must be tried before the lazy fallback
    plain_idx = unmount_script.index("fusermount3 -u /cloud/merged")
    lazy_idx = unmount_script.index("fusermount3 -uz /cloud/merged")
    assert plain_idx < lazy_idx
    assert "||" in unmount_script


def test_reset_upper_refresh_lower_called_between_wipe_and_remount():
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    starting_commands = len(backend.commands)
    calls: list[int] = []

    def _refresh_lower() -> None:
        # Must fire after exactly 2 scripts (reset-unmount + wipe-upper),
        # before the remount (3rd script).
        calls.append(len(backend.commands) - starting_commands)

    mgr.reset_upper(refresh_lower=_refresh_lower)
    assert calls == [2]


def test_reset_upper_raises_on_remount_failure():
    backend = FakeRemoteBackend()
    backend.outputs_by_script["overlay_remount.sh"] = "__SRW_OVERLAY_FAILED__ rc=3\n"
    mgr = _manager(backend)
    mgr.mount()

    with pytest.raises(OverlayMountError):
        mgr.reset_upper(refresh_lower=lambda: None)


# ---------------------------------------------------------------------------
# Session method + POST /cloud-overlay/reset route (status-code contract)
#
# Task 10's orchestrator apply flow trusts these codes: 404 = precondition
# (no overlay to reset — give up), 500 = real failure (retry/alert). The
# manager error types (OverlayMountError, RcloneMountError) both subclass
# RuntimeError, so the route must never branch on RuntimeError — that
# swallowed real failures into the 404 branch (reviewer Critical).
# ---------------------------------------------------------------------------


class _FakeRcloneManager:
    """Stands in for RcloneMountManager: records refresh_vfs calls, optionally
    raising — mirrors the real signature (cloud_mount/__init__.py:222)."""

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    def refresh_vfs(
        self, mount_id: str | None = None, *, recursive: bool = True
    ) -> None:
        self.calls += 1
        if self.error is not None:
            raise self.error


def _stub_session(overlay, rclone):
    """A minimal object carrying the REAL PersistentSession.reset_cloud_overlay
    (bound), so route tests exercise the genuine session→overlay→refresh chain
    without constructing a full PersistentSession."""
    from agent.api.persistent_session import PersistentSession

    class _StubSession:
        reset_cloud_overlay = PersistentSession.reset_cloud_overlay

        def __init__(self) -> None:
            self.overlay_mount_manager = overlay
            self.cloud_mount_manager = rclone
            self.protected_cloud_required = True
            self.protected_workspace_generation = _RESET_WORKSPACE_GENERATION
            self.protected_workspace_runtime_incarnation = _RESET_RUNTIME_INCARNATION

    return _StubSession()


@pytest.fixture
def post_reset(monkeypatch):
    """Install a session stub on the persistent_app module and POST the route."""
    import agent.api.persistent_app as app_mod
    from fastapi.testclient import TestClient

    app = app_mod.create_persistent_app("dummy_config", _RESET_THREAD_ID)
    monkeypatch.setattr(app_mod._session_identity, "_thread_id", _RESET_THREAD_ID)
    monkeypatch.setattr(
        app_mod._session_identity, "_session_generation", _RESET_RUNTIME_GENERATION
    )
    monkeypatch.setattr(app_mod._session_identity, "_attach_token", _RESET_ATTACH_TOKEN)
    monkeypatch.setattr(app_mod, "_registered_pinned_agent_id", lambda: _RESET_AGENT_ID)

    def _post(session, *, headers=None, body=None):
        monkeypatch.setattr(app_mod, "_session", session)
        return TestClient(app).post(
            "/cloud-overlay/reset",
            headers=_reset_headers() if headers is None else headers,
            json=_reset_body() if body is None else body,
        )

    return _post


def test_reset_cloud_overlay_raises_unavailable_when_missing_or_inactive():
    from agent.api.persistent_session import CloudOverlayUnavailable

    # overlay manager present but never mounted -> inactive
    with pytest.raises(CloudOverlayUnavailable):
        _stub_session(
            _manager(FakeRemoteBackend()), _FakeRcloneManager()
        ).reset_cloud_overlay()
    # no overlay manager at all
    with pytest.raises(CloudOverlayUnavailable):
        _stub_session(None, _FakeRcloneManager()).reset_cloud_overlay()


def test_route_404_when_no_session_or_no_active_overlay(post_reset):
    assert post_reset(None).status_code == 404
    # overlay attr exists but the overlay never mounted -> precondition 404
    inactive = _stub_session(_manager(FakeRemoteBackend()), _FakeRcloneManager())
    resp = post_reset(inactive)
    assert resp.status_code == 404
    _assert_sanitized_reset_error(
        resp.json(),
        message="cloud overlay unavailable",
        leaked_exception_text="no active cloud overlay",
    )


def test_route_500_on_overlay_mount_error(post_reset):
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    backend.outputs_by_script["overlay_remount.sh"] = "__SRW_OVERLAY_FAILED__ rc=3\n"

    resp = post_reset(_stub_session(mgr, _FakeRcloneManager()))
    assert resp.status_code == 500
    _assert_sanitized_reset_error(
        resp.json(),
        message="cloud overlay reset failed",
        leaked_exception_text="overlay_remount.sh",
    )


def test_route_500_on_rclone_refresh_error(post_reset):
    """THE reviewer Critical: RcloneMountError subclasses RuntimeError — a
    failed lower vfs/refresh must surface as 500 (retry/alert), never be
    misreported as the 404 give-up branch."""
    from shared.runtime.services.cloud_mount import RcloneMountError

    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    rclone = _FakeRcloneManager(error=RcloneMountError("vfs/refresh failed rc=1"))

    resp = post_reset(_stub_session(mgr, rclone))
    assert resp.status_code == 500
    _assert_sanitized_reset_error(
        resp.json(),
        message="cloud overlay reset failed",
        leaked_exception_text="vfs/refresh failed",
    )


def test_route_success_returns_ok_true(post_reset):
    backend = FakeRemoteBackend()
    mgr = _manager(backend)
    mgr.mount()
    rclone = _FakeRcloneManager()

    resp = post_reset(_stub_session(mgr, rclone))
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert rclone.calls == 1  # lower refreshed exactly once, all mounts


@pytest.mark.parametrize(
    "surface,key",
    [
        ("header", "X-Agent-ID"),
        ("header", "X-Session-Runtime-Generation"),
        ("header", "X-Session-Runtime-Attach-Token"),
        ("body", "thread_id"),
        ("body", "workspace_generation"),
        ("body", "workspace_runtime_incarnation"),
    ],
)
@pytest.mark.parametrize("mutation", ["missing", "stale"])
def test_route_409s_every_missing_or_stale_reset_authority(
    post_reset, surface, key, mutation
):
    headers = _reset_headers()
    body = _reset_body()
    target = headers if surface == "header" else body
    if mutation == "missing":
        target.pop(key)
    else:
        target[key] = "stale-authority"

    session = _stub_session(object(), _FakeRcloneManager())
    session.reset_cloud_overlay = MagicMock()
    resp = post_reset(session, headers=headers, body=body)

    assert resp.status_code == 409
    assert resp.json() == {"error": "stale cloud overlay reset authority"}
    session.reset_cloud_overlay.assert_not_called()


def test_route_rejects_unprotected_session_before_reset(post_reset):
    session = _stub_session(object(), _FakeRcloneManager())
    session.protected_cloud_required = False
    session.reset_cloud_overlay = MagicMock()

    resp = post_reset(session)

    assert resp.status_code == 409
    session.reset_cloud_overlay.assert_not_called()
