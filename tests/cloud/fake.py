"""In-memory ``MainCloudBackend`` for tests.

``FakeMainCloudBackend`` is a deterministic, zero-dependency implementation
of the Protocol suitable for:

* Parametrized contract tests that assert every backend obeys the
  common invariants (``tests/cloud/test_backend_contract.py``).
* Unit tests in ``orchestrator/main.py`` call sites that need a backend
  without standing up a real Nextcloud.

The fake intentionally has no failure modes unless explicitly triggered
by the caller. Tests that want to assert error handling construct the
fake with ``fail_on=...`` or manipulate its state directly.
"""

from __future__ import annotations

from typing import Any, Optional

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    GroupId,
    HealthStatus,
    ProjectFolderEntry,
    ProjectFolderHandle,
    SessionFolderHandle,
    ShareHandle,
    UserHome,
    UserId,
)
from orchestrator.services.cloud.capabilities import (
    MATRIX_ROWS,
    CloudCapability,
    ProviderCapabilities,
)

BACKEND_ID = "fake"

#: The fake offers nothing: a test that needs a capability declares its own.
FAKE_CAPABILITIES = ProviderCapabilities(
    backend_id=BACKEND_ID,
    title="Fake",
    capabilities=tuple(
        CloudCapability(*row, "unsupported", "an in-memory test double")
        for row in MATRIX_ROWS
    ),
)


class FakeMainCloudBackend:
    """Minimal in-memory backend for contract tests."""

    backend_id = BACKEND_ID
    capabilities = FAKE_CAPABILITIES
    attested_fields: frozenset[str] = frozenset()

    def __init__(
        self,
        *,
        start_initialized: bool = True,
        fail_mode: Optional[CloudBackendErrorKind] = None,
    ) -> None:
        self._initialized = start_initialized
        self._backend_instance_id: Optional[str] = None
        self._installation_proof_sha256: Optional[str] = None
        self._fail_mode = fail_mode
        self._users: dict[str, dict[str, str]] = {}
        self._groups: dict[str, set[str]] = {}
        self._project_folders: dict[str, dict[str, object]] = {}
        self._project_files: dict[tuple[str, str], bytes] = {}
        self._session_folders: dict[str, dict[str, object]] = {}
        self._session_files: dict[tuple[str, str], bytes] = {}
        self._shares: dict[str, dict[str, object]] = {}
        self._next_id = 1
        # Tracks every mutating call for assertion in tests.
        self.calls: list[tuple[str, tuple, dict]] = []

    # ------------------------------------------------------------------ Helpers

    def _rec(self, op: str, *args, **kwargs) -> None:
        self.calls.append((op, args, kwargs))

    def _next(self) -> str:
        val = str(self._next_id)
        self._next_id += 1
        return val

    def _fail_if_configured(self) -> None:
        if self._fail_mode is not None:
            raise CloudBackendError(
                self._fail_mode,
                f"fake backend configured to fail with {self._fail_mode.value}",
                backend=self.backend_id,
            )

    def _ensure_ready(self) -> None:
        if not self._initialized:
            raise CloudBackendError(
                CloudBackendErrorKind.UNAVAILABLE,
                "fake backend not initialized",
                backend=self.backend_id,
            )

    def seed_user(
        self, user_id: str, *, email: str = "", display_name: str = ""
    ) -> None:
        """Test helper: pre-populate a user that ``resolve_user_identity`` can find."""
        self._users[user_id] = {"email": email, "display_name": display_name}

    def seed_project_file(
        self, folder_native_id: str, path: str, content: bytes
    ) -> None:
        """Test helper: pre-populate a file inside an already-ensured project folder
        so ``list_project_folder`` / ``get_project_folder_file_bytes`` can return it.
        """
        self._project_files[(folder_native_id, path.strip("/"))] = content

    # --------------------------------------------------------------- Properties

    @property
    def is_configured(self) -> bool:
        return True

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    @property
    def backend_instance_id(self) -> Optional[str]:
        return self._backend_instance_id

    @property
    def installation_proof_sha256(self) -> Optional[str]:
        return self._installation_proof_sha256

    def bind_backend_instance(self, backend_instance_id: str) -> None:
        if self._backend_instance_id not in {None, backend_instance_id}:
            raise RuntimeError("fake backend instance authority changed")
        self._backend_instance_id = backend_instance_id

    def prepare_backend_instance_attestation(self, backend_instance_id: str) -> None:
        if not isinstance(backend_instance_id, str) or not backend_instance_id:
            raise ValueError("fake attestation instance is missing")

    @property
    def webdav_credentials(self) -> dict[str, str]:
        return {"username": "fake-agent", "password": "fake-password"}

    def cloud_sync_config(
        self, webdav_url: str, *, target_user_sub: Optional[str] = None
    ) -> Optional[dict[str, Any]]:
        return {
            "backend": self.backend_id,
            "webdav_url": webdav_url,
            "auth": {"type": "basic", **self.webdav_credentials},
        }

    def legacy_folder_id(self, handle: ProjectFolderHandle) -> Optional[int]:
        return None

    # ---------------------------------------------------------------- Lifecycle

    async def health_check(self) -> HealthStatus:
        return HealthStatus(ok=self._initialized, latency_ms=0.0)

    async def ensure_initialized(self) -> bool:
        self._initialized = True
        return True

    async def close(self) -> None:
        self._initialized = False

    # ------------------------------------------------------------------ Identity

    async def resolve_user_identity(
        self, email: Optional[str], display_name: Optional[str]
    ) -> Optional[UserId]:
        self._ensure_ready()
        self._fail_if_configured()
        for uid, rec in self._users.items():
            if email and rec.get("email", "").lower() == email.lower():
                return UserId(uid)
            if (
                display_name
                and rec.get("display_name", "").lower() == display_name.lower()
            ):
                return UserId(uid)
        return None

    async def ensure_user(
        self,
        *,
        sub: str,
        issuer: str,
        email: Optional[str],
        display_name: Optional[str],
        preferred_username: Optional[str] = None,
    ) -> Optional[UserId]:
        self._ensure_ready()
        self._fail_if_configured()
        existing = await self.resolve_user_identity(email, display_name)
        if existing:
            return existing
        if not email and not display_name:
            return None
        uid = sub
        self._users[uid] = {"email": email or "", "display_name": display_name or ""}
        return UserId(uid)

    async def get_user_home(self, user_id: UserId) -> Optional[UserHome]:
        if not self._initialized:
            return None
        handle = ProjectFolderHandle(
            backend=self.backend_id,
            native_id=f"home:{user_id}",
            vendor_meta={"kind": "user_home", "username": str(user_id)},
        )
        return UserHome(
            handle=handle,
            browser_url=f"fake://{user_id}/home",
            webdav_url=f"fake-dav://{user_id}/",
        )

    def get_default_home_browser_url(self) -> Optional[str]:
        return "fake://home"

    # -------------------------------------------------------------------- Groups

    async def ensure_group(self, group_id: GroupId) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        self._rec("ensure_group", group_id)
        self._groups.setdefault(str(group_id), set())

    async def add_user_to_group(self, user_id: UserId, group_id: GroupId) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        self._rec("add_user_to_group", user_id, group_id)
        self._groups.setdefault(str(group_id), set()).add(str(user_id))

    async def remove_user_from_group(self, user_id: UserId, group_id: GroupId) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        self._rec("remove_user_from_group", user_id, group_id)
        self._groups.get(str(group_id), set()).discard(str(user_id))

    # ------------------------------------------------------------- Project folders

    async def ensure_project_folder(
        self, *, project_name: str, group_id: GroupId
    ) -> ProjectFolderHandle:
        self._ensure_ready()
        self._fail_if_configured()
        # Idempotent: if a folder with this (project_name, group_id) already
        # exists, return the existing handle.
        for fid, folder in self._project_folders.items():
            if folder["project_name"] == project_name and folder["group_id"] == str(
                group_id
            ):
                return ProjectFolderHandle(
                    backend=self.backend_id,
                    native_id=fid,
                    vendor_meta={"mountpoint": project_name},
                )
        folder_id = self._next()
        self._project_folders[folder_id] = {
            "project_name": project_name,
            "group_id": str(group_id),
        }
        self._rec("ensure_project_folder", project_name, group_id)
        return ProjectFolderHandle(
            backend=self.backend_id,
            native_id=folder_id,
            vendor_meta={"mountpoint": project_name},
        )

    async def delete_project_folder(
        self, handle: ProjectFolderHandle, *, if_exists: bool = True
    ) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        self._rec("delete_project_folder", handle.native_id)
        if handle.native_id not in self._project_folders:
            if if_exists:
                return
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"project folder {handle.native_id} not found",
                backend=self.backend_id,
            )
        del self._project_folders[handle.native_id]

    async def refresh_project_folder_access(
        self, handle: ProjectFolderHandle, group_id: GroupId
    ) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        self._rec("refresh_project_folder_access", handle.native_id, group_id)

    def get_project_folder_browser_url(
        self, handle: ProjectFolderHandle
    ) -> Optional[str]:
        mountpoint = handle.vendor_meta.get("mountpoint")
        if not mountpoint:
            return None
        return f"fake://folder/{mountpoint}"

    def get_project_folder_webdav_url(
        self, handle: ProjectFolderHandle
    ) -> Optional[str]:
        mountpoint = handle.vendor_meta.get("mountpoint")
        if not mountpoint:
            return None
        return f"fake-dav://folder/{mountpoint}/"

    async def list_project_folder(
        self,
        handle: ProjectFolderHandle,
        *,
        subpath: str = "",
    ) -> list[ProjectFolderEntry]:
        self._ensure_ready()
        self._fail_if_configured()
        if handle.native_id not in self._project_folders:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"project folder {handle.native_id} not found",
                backend=self.backend_id,
            )
        prefix = subpath.strip("/")
        entries: list[ProjectFolderEntry] = []
        dirs: set[str] = set()
        for (fid, path), content in self._project_files.items():
            if fid != handle.native_id:
                continue
            if prefix and not (path == prefix or path.startswith(prefix + "/")):
                continue
            rel = path[len(prefix) + 1 :] if prefix else path
            entries.append(
                ProjectFolderEntry(path=rel, is_dir=False, size=len(content))
            )
            parts = rel.split("/")[:-1]
            for i in range(1, len(parts) + 1):
                dirs.add("/".join(parts[:i]))
        for d in dirs:
            entries.append(ProjectFolderEntry(path=d, is_dir=True))
        entries.sort(key=lambda e: e.path)
        return entries

    async def capture_etag_baseline(
        self, handle: ProjectFolderHandle
    ) -> dict[str, str]:
        entries = await self.list_project_folder(handle)
        return {e.path: e.etag for e in entries if not e.is_dir}

    async def get_project_folder_file_bytes(
        self,
        handle: ProjectFolderHandle,
        *,
        path: str,
    ) -> bytes:
        self._ensure_ready()
        self._fail_if_configured()
        rel = path.lstrip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "get_project_folder_file_bytes: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        blob = self._project_files.get((handle.native_id, rel))
        if blob is None:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"project folder file {handle.native_id}/{rel} not found",
                backend=self.backend_id,
            )
        return blob

    async def put_project_folder_file_bytes(
        self,
        handle: ProjectFolderHandle,
        *,
        path: str,
        content: bytes,
        content_type: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "put_project_folder_file_bytes: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        if handle.native_id not in self._project_folders:
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"project folder {handle.native_id} not found",
                backend=self.backend_id,
            )
        self._project_files[(handle.native_id, rel)] = content
        self._rec("put_project_folder_file_bytes", handle.native_id, rel, content_type)

    async def delete_project_folder_file(
        self,
        handle: ProjectFolderHandle,
        *,
        path: str,
        if_exists: bool = True,
    ) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "delete_project_folder_file: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        key = (handle.native_id, rel)
        self._rec("delete_project_folder_file", handle.native_id, rel)
        if key not in self._project_files:
            if if_exists:
                return
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"project folder file {handle.native_id}/{rel} not found",
                backend=self.backend_id,
            )
        del self._project_files[key]

    # ------------------------------------------------------------- Session folders

    async def ensure_session_folder(self, *, session_id: str) -> SessionFolderHandle:
        self._ensure_ready()
        self._fail_if_configured()
        # Idempotent
        for existing_id in self._session_folders:
            if existing_id == session_id:
                return SessionFolderHandle(
                    backend=self.backend_id,
                    native_id=f"sessions/{session_id}",
                )
        self._session_folders[session_id] = {"created": True}
        self._rec("ensure_session_folder", session_id)
        return SessionFolderHandle(
            backend=self.backend_id,
            native_id=f"sessions/{session_id}",
        )

    async def delete_session_folder(
        self, handle: SessionFolderHandle, *, if_exists: bool = True
    ) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        self._rec("delete_session_folder", handle.native_id)
        key = handle.native_id.removeprefix("sessions/")
        if key not in self._session_folders:
            if if_exists:
                return
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"session folder {handle.native_id} not found",
                backend=self.backend_id,
            )
        del self._session_folders[key]

    async def share_session_folder(
        self, handle: SessionFolderHandle, user_id: UserId
    ) -> ShareHandle:
        self._ensure_ready()
        self._fail_if_configured()
        share_id = self._next()
        self._shares[share_id] = {
            "folder": handle.native_id,
            "user_id": str(user_id),
        }
        self._rec("share_session_folder", handle.native_id, user_id)
        return ShareHandle(backend=self.backend_id, native_id=share_id)

    async def revoke_session_share(
        self, share: ShareHandle, *, if_exists: bool = True
    ) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        self._rec("revoke_session_share", share.native_id)
        if share.native_id not in self._shares:
            if if_exists:
                return
            raise CloudBackendError(
                CloudBackendErrorKind.NOT_FOUND,
                f"share {share.native_id} not found",
                backend=self.backend_id,
            )
        del self._shares[share.native_id]

    def get_session_folder_browser_url(
        self, handle: SessionFolderHandle
    ) -> Optional[str]:
        return f"fake://session/{handle.native_id}"

    def get_session_folder_webdav_url(
        self, handle: SessionFolderHandle
    ) -> Optional[str]:
        return f"fake-dav://session/{handle.native_id}/"

    async def put_session_file(
        self,
        handle: SessionFolderHandle,
        *,
        path: str,
        content: bytes,
        content_type: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        self._fail_if_configured()
        rel = path.strip("/")
        if not rel:
            raise CloudBackendError(
                CloudBackendErrorKind.INVALID_REQUEST,
                "put_session_file: path must be non-empty",
                backend=self.backend_id,
                retryable=False,
            )
        self._rec("put_session_file", handle.native_id, rel, content_type)
        self._session_files[(handle.native_id, rel)] = content
