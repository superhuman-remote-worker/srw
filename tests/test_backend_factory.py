"""Tests for the lite-backend factory (keyed on workspace.backend).

The factory is what the worker/session bootstrap seams call once they detect a
LITE_BACKENDS value. It uses a SimpleNamespace stand-in for the workspace
config so it stays decoupled from the loader dataclass — the factory only needs
``.backend`` and ``.mounts``. The 'memory' object-store type lets virtual mode
be exercised end-to-end here without rclone or a network.
"""

from types import SimpleNamespace

import pytest

from agent.core.backends.factory import (  # noqa: E402
    LITE_BACKENDS,
    create_lite_backend,
    is_lite_backend,
)
from agent.core.backends.scratch import ScratchBackend  # noqa: E402
from agent.core.backends.virtual import VirtualWorkspaceBackend  # noqa: E402


class TestLiteBackendSet:
    def test_lite_backends_membership(self):
        assert is_lite_backend("virtual") is True
        assert is_lite_backend("none") is True

    def test_sandbox_and_vm_are_not_lite(self):
        assert is_lite_backend("sandbox") is False
        assert is_lite_backend("vm") is False
        assert "sandbox" not in LITE_BACKENDS
        assert "vm" not in LITE_BACKENDS


class TestNoneMode:
    def test_creates_scratch_backend(self):
        cfg = SimpleNamespace(backend="none", mounts=None)
        b = create_lite_backend(cfg, job_id="job-1")
        try:
            assert isinstance(b, ScratchBackend)
        finally:
            b.disconnect()


class TestVirtualMode:
    def test_creates_virtual_backend_with_prefix(self):
        cfg = SimpleNamespace(
            backend="virtual",
            mounts=[{"rclone_spec": {"type": "memory"}, "prefix": "jobs/x/"}],
        )
        b = create_lite_backend(cfg, job_id="job-1")
        assert isinstance(b, VirtualWorkspaceBackend)
        assert b.root == "jobs/x/"

    def test_virtual_backend_is_wired_to_store(self):
        cfg = SimpleNamespace(
            backend="virtual",
            mounts=[{"rclone_spec": {"type": "memory"}, "prefix": "jobs/x/"}],
        )
        b = create_lite_backend(cfg, job_id="job-1")
        b.write_file("hello.txt", "world")
        assert b.read_file("hello.txt") == "world"

    def test_default_prefix_from_job_id_when_absent(self):
        cfg = SimpleNamespace(
            backend="virtual",
            mounts=[{"rclone_spec": {"type": "memory"}}],
        )
        b = create_lite_backend(cfg, job_id="abc-123")
        assert b.root == "jobs/abc-123/"

    def test_virtual_without_mounts_raises(self):
        cfg = SimpleNamespace(backend="virtual", mounts=None)
        with pytest.raises(ValueError, match="requires workspace.mounts"):
            create_lite_backend(cfg, job_id="job-1")


class TestNonLiteRejected:
    def test_sandbox_rejected(self):
        cfg = SimpleNamespace(backend="sandbox", mounts=None)
        with pytest.raises(ValueError, match="non-lite backend"):
            create_lite_backend(cfg, job_id="job-1")


class TestVirtualCloudFolders:
    """Entries after the first mount are cloud folders under ``cloud/<name>``
    (features/cloud_folders_on_virtual_sessions.md § Agent)."""

    def _cfg(self, *entries):
        return SimpleNamespace(
            backend="virtual",
            mounts=[
                {
                    "name": "workspace",
                    "rclone_spec": {"type": "memory"},
                    "prefix": "threads/t/",
                },
                *entries,
            ],
        )

    def test_each_entry_becomes_a_folder_over_its_own_store(self):
        b = create_lite_backend(
            self._cfg(
                {
                    "name": "outbox",
                    "mount_id": "c-1",
                    "mount_kind": "cloud_outbox",
                    "access": "read_write",
                    "shared": False,
                    "rclone_spec": {"type": "memory", "config": {}, "root": ""},
                },
                {
                    "name": "cloud/proj",
                    "mount_id": "c-2",
                    "mount_kind": "project",
                    "access": "read_only",
                    "rclone_spec": {"type": "memory"},
                },
            ),
            job_id="t",
        )
        assert b.root == "threads/t/"
        rows = b.cloud_folders()
        assert [
            (r["name"], r["path"], r["access"], r["kind"], r["mount_id"]) for r in rows
        ] == [
            ("outbox", "cloud/outbox", "read_write", "cloud_outbox", "c-1"),
            ("proj", "cloud/proj", "read_only", "project", "c-2"),
        ]
        assert rows[0]["shared"] is False and rows[1]["shared"] is None
        b.write_file("cloud/outbox/r.md", "r")
        b.write_file("r.md", "w")
        stores = [r.store for r in b.cloud_remotes]
        assert stores[0] is not stores[1]
        assert stores[0].get("r.md") == b"r" and stores[1].list("") == []

    def test_cloud_folders_false_binds_the_workspace_only(self):
        cfg = self._cfg({"name": "outbox", "rclone_spec": {"type": "memory"}})
        b = create_lite_backend(cfg, job_id="t", cloud_folders=False)
        assert b.cloud_remotes == []
        assert b.list_dir("") == []

    def test_an_entry_without_a_name_or_type_is_refused(self):
        with pytest.raises(ValueError, match="no usable name"):
            create_lite_backend(
                self._cfg({"rclone_spec": {"type": "memory"}}), job_id="t"
            )
        with pytest.raises(ValueError, match="no rclone remote type"):
            create_lite_backend(self._cfg({"name": "x", "rclone_spec": {}}), job_id="t")

    def test_rclone_folders_get_distinct_remote_aliases(self):
        from shared.runtime.core.backends.rclone import RcloneObjectStore

        b = create_lite_backend(
            self._cfg(
                {
                    "name": "outbox",
                    "rclone_spec": {
                        "type": "webdav",
                        "config": {
                            "url": "https://c/remote.php/dav",
                            "user": "svc",
                            "pass": "x",
                        },
                        "root": "sessions/t",
                    },
                },
                {
                    "name": "my_cloud",
                    "rclone_spec": {
                        "type": "webdav",
                        "config": {
                            "url": "https://c/remote.php/dav",
                            "user": "me",
                            "pass": "y",
                        },
                        "root": "",
                    },
                },
            ),
            job_id="t",
        )
        stores = [r.store for r in b.cloud_remotes]
        assert all(isinstance(s, RcloneObjectStore) for s in stores)
        envs = [s._env_overlay for s in stores]
        assert "RCLONE_CONFIG_CLOUD_OUTBOX_PASS" in envs[0]
        assert "RCLONE_CONFIG_CLOUD_MY_CLOUD_PASS" in envs[1]
        assert stores[0]._remote_path("r.md") == "cloud_outbox:sessions/t/r.md"
        assert stores[1]._remote_path("r.md") == "cloud_my_cloud:r.md"
