"""Contract tests for every ``MainCloudBackend`` implementation.

Parametrized over ``FakeMainCloudBackend`` in Phase 1.5. Phase 2 adds the
``opencloud`` row; a ``nextcloud`` row lands when VCR cassettes are recorded
against a local Nextcloud.

The invariants asserted here are the contract every backend must honor.
New backends get tested "for free" by adding a fixture entry to
``_backend_fixtures``.
"""

from __future__ import annotations

import json
from typing import Callable

import pytest

from orchestrator.services.cloud import (
    CloudBackendError,
    CloudBackendErrorKind,
    GroupId,
    MainCloudBackend,
    ProjectFolderHandle,
    SessionFolderHandle,
    ShareHandle,
)
from orchestrator.services.cloud.handles import _deserialize, _serialize

from tests.cloud.fake import FakeMainCloudBackend


# --- Fixtures ---------------------------------------------------------------

BackendFactory = Callable[[], MainCloudBackend]

_backend_fixtures: list[tuple[str, BackendFactory]] = [
    ("fake", lambda: FakeMainCloudBackend(start_initialized=True)),
]


@pytest.fixture(
    params=_backend_fixtures,
    ids=[name for name, _ in _backend_fixtures],
)
def backend(request) -> MainCloudBackend:
    _name, factory = request.param
    return factory()


# --- Static contract tests (not parametrized over backend) ------------------


class TestProtocolSatisfaction:
    """Every registered backend must structurally implement MainCloudBackend."""

    def test_fake_satisfies_protocol(self):
        be = FakeMainCloudBackend()
        assert isinstance(be, MainCloudBackend)

    def test_fake_has_required_properties(self):
        be = FakeMainCloudBackend()
        assert isinstance(be.backend_id, str)
        assert isinstance(be.is_configured, bool)
        assert isinstance(be.is_initialized, bool)
        assert be.backend_instance_id is None
        assert be.installation_proof_sha256 is None
        assert isinstance(be.webdav_credentials, dict)


class TestHandleRoundTrip:
    """Handles must round-trip through to_db / from_db losslessly."""

    def test_project_handle_bare_string(self):
        h = ProjectFolderHandle(backend="nextcloud", native_id="42")
        s = h.to_db()
        assert s == "42"
        h2 = ProjectFolderHandle.from_db(s, backend="nextcloud")
        assert h2 == h

    def test_project_handle_with_vendor_meta(self):
        h = ProjectFolderHandle(
            backend="opencloud",
            native_id="storage!space",
            vendor_meta={"webdav_url": "https://cloud.example.com/dav/"},
        )
        s = h.to_db()
        data = json.loads(s)
        assert data["backend"] == "opencloud"
        assert data["native_id"] == "storage!space"
        h2 = ProjectFolderHandle.from_db(s, backend="opencloud")
        assert h2 == h

    def test_session_handle_round_trip(self):
        h = SessionFolderHandle(backend="nextcloud", native_id="sessions/abc")
        assert h == SessionFolderHandle.from_db(h.to_db(), backend="nextcloud")

    def test_share_handle_round_trip(self):
        h = ShareHandle(backend="nextcloud", native_id="99")
        assert h == ShareHandle.from_db(h.to_db(), backend="nextcloud")

    def test_from_db_falls_back_on_malformed_json(self):
        """If the TEXT column contains garbage that looks like JSON but isn't,
        we recover as a bare-string handle rather than crashing."""
        h = ProjectFolderHandle.from_db("{not valid json", backend="nextcloud")
        assert h.backend == "nextcloud"
        assert h.native_id == "{not valid json"
        assert h.vendor_meta == {}

    def test_serialize_empty_vendor_meta_is_bare_string(self):
        backend, nid, meta = _deserialize(
            _serialize("nextcloud", "42", {}), backend="nextcloud"
        )
        assert (backend, nid, meta) == ("nextcloud", "42", {})

    def test_serialize_with_vendor_meta_is_json(self):
        s = _serialize("opencloud", "foo!bar", {"key": "value"})
        assert s.startswith("{")


# --- Parametrized behavioral contract ---------------------------------------


class TestBackendContract:
    """Every backend must pass these behavioral assertions."""

    @pytest.mark.asyncio
    async def test_backend_id_is_string(self, backend: MainCloudBackend):
        assert isinstance(backend.backend_id, str)
        assert backend.backend_id != ""

    @pytest.mark.asyncio
    async def test_ensure_project_folder_idempotent(self, backend: MainCloudBackend):
        h1 = await backend.ensure_project_folder(
            project_name="ContractTest", group_id=GroupId("project-1")
        )
        h2 = await backend.ensure_project_folder(
            project_name="ContractTest", group_id=GroupId("project-1")
        )
        # Same native_id for the same (project_name, group_id)
        assert h1.native_id == h2.native_id
        assert h1.backend == backend.backend_id

    @pytest.mark.asyncio
    async def test_ensure_session_folder_idempotent(self, backend: MainCloudBackend):
        h1 = await backend.ensure_session_folder(session_id="abc123")
        h2 = await backend.ensure_session_folder(session_id="abc123")
        assert h1.native_id == h2.native_id
        assert h1.backend == backend.backend_id

    @pytest.mark.asyncio
    async def test_an_exclusive_session_folder_is_never_one_already_there(
        self, backend: MainCloudBackend
    ):
        fresh = await backend.ensure_session_folder(session_id="new-1", exclusive=True)
        assert fresh.native_id == "sessions/new-1"
        with pytest.raises(CloudBackendError) as exc:
            await backend.ensure_session_folder(session_id="new-1", exclusive=True)
        assert exc.value.kind == CloudBackendErrorKind.ALREADY_EXISTS

    @pytest.mark.asyncio
    async def test_delete_project_folder_no_op_on_missing(
        self, backend: MainCloudBackend
    ):
        missing = ProjectFolderHandle(backend=backend.backend_id, native_id="99999")
        # if_exists=True (default) must swallow NOT_FOUND
        await backend.delete_project_folder(missing)

    @pytest.mark.asyncio
    async def test_delete_project_folder_raises_when_if_exists_false(
        self, backend: MainCloudBackend
    ):
        missing = ProjectFolderHandle(backend=backend.backend_id, native_id="99999")
        with pytest.raises(CloudBackendError) as exc_info:
            await backend.delete_project_folder(missing, if_exists=False)
        assert exc_info.value.kind == CloudBackendErrorKind.NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_session_folder_no_op_on_missing(
        self, backend: MainCloudBackend
    ):
        missing = SessionFolderHandle(
            backend=backend.backend_id, native_id="sessions/never-existed"
        )
        await backend.delete_session_folder(missing)  # must not raise

    @pytest.mark.asyncio
    async def test_revoke_session_share_no_op_on_missing(
        self, backend: MainCloudBackend
    ):
        missing = ShareHandle(backend=backend.backend_id, native_id="99999")
        await backend.revoke_session_share(missing)  # must not raise

    @pytest.mark.asyncio
    async def test_group_ops_are_idempotent(self, backend: MainCloudBackend):
        await backend.ensure_group(GroupId("contract-test-group"))
        await backend.ensure_group(GroupId("contract-test-group"))  # no error

    @pytest.mark.asyncio
    async def test_resolve_user_identity_returns_none_for_unknown(
        self, backend: MainCloudBackend
    ):
        result = await backend.resolve_user_identity("nobody@example.com", "")
        assert result is None

    @pytest.mark.asyncio
    async def test_webdav_credentials_is_dict(self, backend: MainCloudBackend):
        creds = backend.webdav_credentials
        assert isinstance(creds, dict)
        # WebDAV backends return username + password; Graph backends return {}
        if creds:
            assert "username" in creds
            assert "password" in creds

    @pytest.mark.asyncio
    async def test_project_folder_urls_from_handle(self, backend: MainCloudBackend):
        handle = await backend.ensure_project_folder(
            project_name="UrlTest", group_id=GroupId("project-2")
        )
        browser_url = backend.get_project_folder_browser_url(handle)
        webdav_url = backend.get_project_folder_webdav_url(handle)
        # Browser URL is always populated when the handle has the mountpoint
        assert browser_url is not None
        # WebDAV URL may be None on Graph backends; Nextcloud/Fake always set it
        assert webdav_url is not None or backend.backend_id == "ms365"

    @pytest.mark.asyncio
    async def test_default_home_browser_url_is_string(self, backend: MainCloudBackend):
        url = backend.get_default_home_browser_url()
        assert url is None or isinstance(url, str)


# --- Error-mapping sanity ----------------------------------------------------


class TestErrorContract:
    """CloudBackendError round-tripping and enum coverage."""

    def test_error_kinds_cover_all_http_classes(self):
        # If a new HTTP error class shows up and doesn't have a kind, the
        # adapter falls back to UNKNOWN — verify that path is covered.
        kinds = {k.value for k in CloudBackendErrorKind}
        required = {
            "not_found",
            "already_exists",
            "permission_denied",
            "authentication_failed",
            "throttled",
            "timeout",
            "unavailable",
            "invalid_request",
            "not_supported",
            "unknown",
        }
        missing = required - kinds
        assert not missing, f"missing error kinds: {missing}"

    def test_error_repr_includes_backend_and_kind(self):
        err = CloudBackendError(
            CloudBackendErrorKind.NOT_FOUND,
            "test",
            backend="nextcloud",
        )
        s = str(err)
        assert "nextcloud" in s
        assert "not_found" in s

    @pytest.mark.asyncio
    async def test_configured_fail_mode_raises(self):
        backend = FakeMainCloudBackend(
            start_initialized=True,
            fail_mode=CloudBackendErrorKind.PERMISSION_DENIED,
        )
        with pytest.raises(CloudBackendError) as exc_info:
            await backend.ensure_group(GroupId("nope"))
        assert exc_info.value.kind == CloudBackendErrorKind.PERMISSION_DENIED
        assert exc_info.value.backend == "fake"

    @pytest.mark.asyncio
    async def test_uninitialized_backend_raises_unavailable(self):
        backend = FakeMainCloudBackend(start_initialized=False)
        with pytest.raises(CloudBackendError) as exc_info:
            await backend.ensure_group(GroupId("nope"))
        assert exc_info.value.kind == CloudBackendErrorKind.UNAVAILABLE
