"""Test double — ``WorkspaceSyncBase`` implementation backed by a tmpdir.

Lets ``WorkspaceSyncCoordinator`` integration tests round-trip files end-to-end
without standing up a WebDAV server. The "remote" side is just another
directory on disk; etags are deterministic content hashes so the pull
algorithm's "did this file change" branch is testable in CI.

Lives under ``tests/`` (not ``src/``) on purpose — production code never
points at a local FS as its workspace cloud. Mirrors the convention used
by ``tests/_fs_backend.py`` for ``WorkspaceManager``.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Awaitable, Callable, Optional

from agent.services.cloud_sync.base import CloudSyncFenceLost, WorkspaceSyncBase


class LocalFsWorkspaceSync(WorkspaceSyncBase):
    """Cloud-side is a local directory; primitives just copy files around."""

    def __init__(
        self,
        workspace_path: Path,
        *,
        remote_root: Path,
        poll_interval: int = 15,
        workspace_backend=None,
        mount_subdir: str = "",
    ) -> None:
        super().__init__(
            workspace_path,
            poll_interval=poll_interval,
            workspace_backend=workspace_backend,
            mount_subdir=mount_subdir,
        )
        self.conditional_writes: list[tuple[str, Optional[str], bool]] = []
        self._remote_root = Path(remote_root)
        self._remote_root.mkdir(parents=True, exist_ok=True)

    @property
    def remote_root(self) -> Path:
        return self._remote_root

    async def _ensure_ready(self) -> None:
        return None

    async def _ensure_remote_dir(
        self,
        rel_dir: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
    ) -> None:
        if before_write is not None:
            await before_write()
        if rel_dir and rel_dir != ".":
            (self._remote_root / rel_dir).mkdir(parents=True, exist_ok=True)

    def _etag_of(self, rel_path: str) -> Optional[str]:
        p = self._remote_root / rel_path
        if not p.is_file():
            return None
        return hashlib.sha256(p.read_bytes()).hexdigest()

    async def _upload_file(
        self,
        rel_path: str,
        local_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False,
    ) -> Optional[str]:
        if before_write is not None:
            await before_write()
        current = self._etag_of(rel_path)
        # RFC 4918 preconditions, exactly as a WebDAV server applies them.
        if if_match and current != if_match:
            raise CloudSyncFenceLost(f"If-Match failed for {rel_path}")
        if if_none_match and current is not None:
            raise CloudSyncFenceLost(f"If-None-Match: * failed for {rel_path}")
        self.conditional_writes.append((rel_path, if_match, if_none_match))
        dst = self._remote_root / rel_path
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(local_path, dst)
        return self._etag_of(rel_path)

    async def _delete_remote_file(
        self,
        rel_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
    ) -> None:
        if before_write is not None:
            await before_write()
        current = self._etag_of(rel_path)
        if if_match and current is not None and current != if_match:
            raise CloudSyncFenceLost(f"If-Match failed for delete of {rel_path}")
        (self._remote_root / rel_path).unlink(missing_ok=True)

    async def _remote_etag(self, rel_path: str) -> Optional[str]:
        return self._etag_of(rel_path)

    async def _list_remote_files(self, rel_dir: str = "") -> list[dict]:
        # Deliberately ignores ``rel_dir`` and lists recursively — the base
        # tree walk dedups, and this double exercises exactly that tolerance.
        out: list[dict] = []
        if not self._remote_root.exists():
            return out
        for p in self._remote_root.rglob("*"):
            if p.is_dir():
                continue
            rel = str(p.relative_to(self._remote_root))
            etag = hashlib.sha256(p.read_bytes()).hexdigest()
            out.append(
                {"path": rel, "etag": etag, "isdir": False, "size": p.stat().st_size}
            )
        return out

    async def _download_file(self, rel_path: str, local_path: str) -> None:
        src = self._remote_root / rel_path
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, local_path)


class FailingLocalFsWorkspaceSync(LocalFsWorkspaceSync):
    """Variant that raises on either pull or push.

    Used to exercise the coordinator's raise-and-block aggregation policy.
    """

    def __init__(
        self,
        workspace_path: Path,
        *,
        remote_root: Path,
        fail_on: str = "push",
        message: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(workspace_path, remote_root=remote_root, **kwargs)
        self._fail_on = fail_on
        self._message = message or f"forced {fail_on} failure"

    async def _upload_file(
        self,
        rel_path: str,
        local_path: str,
        *,
        before_write: Optional[Callable[[], Awaitable[None]]] = None,
        if_match: Optional[str] = None,
        if_none_match: bool = False,
    ) -> Optional[str]:
        # ``_ensure_remote_dirs`` deliberately swallows mkdir errors (the
        # upload below reports the real one), so failing on dir creation
        # won't surface to the strict-mode caller. Failing on the actual
        # upload is what the algorithm propagates.
        if self._fail_on == "push":
            raise RuntimeError(self._message)
        await super()._upload_file(rel_path, local_path, before_write=before_write)

    async def _list_remote_files(self, rel_dir: str = "") -> list[dict]:
        if self._fail_on == "pull":
            raise RuntimeError(self._message)
        return await super()._list_remote_files(rel_dir)
