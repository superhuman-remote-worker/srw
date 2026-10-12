"""Regression tests for the ``/api/uploads`` router gates.

Security audit 2026-08-27: the five upload routes took no identity of any
kind, ``DELETE /api/uploads/{upload_id}`` built ``rmtree(UPLOADS_DIR /
upload_id)`` from an unvalidated path segment, ``POST`` buffered every file
whole before checking its size, and ``POST /api/jobs`` copied any upload id
into the job context with no ownership check. Each test here fails on the
pre-fix router.

The router is exercised through a minimal FastAPI app (only ``uploads.router``
mounted) so the gate under test is the router's own, not something main.py's
middleware stack adds. ``create_job`` is exercised directly, the way
``tests/test_internal_auth.py`` does.
"""

import json
import secrets
import time
from contextlib import ExitStack
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import orchestrator.security.access as access_module
import orchestrator.uploads

INTERNAL_KEY = "test-internal-key"
_SESSIONS = {"sess-a": "user_a", "sess-b": "user_b", "sess-admin": "user_admin"}


# =============================================================================
# Helpers / fixtures
# =============================================================================


def _mint(upload_type: str = "documents") -> str:
    """An id shaped exactly as ``upload_files`` mints them."""
    return f"{upload_type}_{int(time.time() * 1000)}_{secrets.token_hex(8)}"


def _write_upload(uploads_dir, upload_id, *, owner, files=(("doc.txt", b"hello"),)):
    """Lay an upload down on disk; ``owner=None`` mimics a pre-binding upload."""
    upload_dir = uploads_dir / upload_id
    upload_dir.mkdir(parents=True)
    for name, data in files:
        (upload_dir / name).write_bytes(data)
    metadata = {
        "upload_id": upload_id,
        "upload_type": upload_id.split("_", 1)[0],
        "files": [
            {"name": name, "size": len(data), "mime_type": "text/plain"}
            for name, data in files
        ],
        "created_at": "2026-09-02T00:00:00",
    }
    if owner is not None:
        metadata["user_id"] = owner
    (upload_dir / "metadata.json").write_text(json.dumps(metadata))
    return upload_dir


def _cookie(session_id: str) -> dict[str, str]:
    return {"Cookie": f"srw_session={session_id}"}


def _internal() -> dict[str, str]:
    return {"X-Internal-Key": INTERNAL_KEY}


@pytest.fixture
def uploads_dir(tmp_path, monkeypatch):
    target = tmp_path / "uploads"
    monkeypatch.setattr(orchestrator.uploads, "UPLOADS_DIR", target)
    return target


@pytest.fixture
def internal_key(monkeypatch):
    """Pin the shared key so ``is_internal_call`` is deterministic under a
    leaked ``MCP_INTERNAL_KEY`` env and a missing header is never internal."""
    monkeypatch.setattr(access_module, "_INTERNAL_KEY", INTERNAL_KEY)
    return INTERNAL_KEY


@pytest.fixture
def cookie_users(user_a, user_b, user_admin, monkeypatch):
    """Resolve the ``srw_session`` cookie to a user without Keycloak.

    Only the cookie→session lookup is stubbed; ``get_current_user`` and
    ``require_approved_user`` run for real, so a missing cookie still walks
    the anonymous path to its 401.
    """
    users = {"user_a": user_a, "user_b": user_b, "user_admin": user_admin}
    sessions = {sid: users[name] for sid, name in _SESSIONS.items()}

    async def _resolve(session_id, db):
        return sessions.get(session_id)

    monkeypatch.setattr("orchestrator.security.auth._resolve_from_cookie", _resolve)
    return users


@pytest.fixture
def app(uploads_dir, internal_key, cookie_users):
    application = FastAPI()
    application.include_router(orchestrator.uploads.router)
    # The router reads its store from the application handling the request
    # (`app.state.store`), not from `orchestrator.main` — R1.B03 closed that
    # caller. Publishing it here is what composition does in production.
    application.state.store = MagicMock()
    return application


@pytest.fixture
def client(app):
    return TestClient(app)


@pytest.fixture
def rmtree_guard(monkeypatch):
    """Fail the test if anything reaches ``shutil.rmtree``."""
    guard = MagicMock(side_effect=AssertionError("rmtree reached the filesystem"))
    monkeypatch.setattr(orchestrator.uploads.shutil, "rmtree", guard)
    return guard


async def _raw_asgi(app, method: str, path: str, headers: dict[str, str]):
    """Drive the app with a verbatim path.

    httpx (and so TestClient) normalises ``/api/uploads/..`` to ``/api``
    before it leaves the client; a raw HTTP client (``curl --path-as-is``)
    does not, so the server has to be shown the literal dot segments.
    """
    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()],
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
    }
    status: dict[str, int] = {}
    body = bytearray()

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        if message["type"] == "http.response.start":
            status["code"] = message["status"]
        elif message["type"] == "http.response.body":
            body.extend(message.get("body", b""))

    await app(scope, receive, send)
    return status["code"], bytes(body)


# =============================================================================
# Anonymous callers are refused on every route
# =============================================================================


class TestAnonymousRefused:
    def test_post_is_401(self, client, uploads_dir):
        response = client.post("/api/uploads", files={"files": ("a.txt", b"x")})
        assert response.status_code == 401
        assert not uploads_dir.exists() or not list(uploads_dir.iterdir())

    @pytest.mark.parametrize("upload_type", ["config", "instructions"])
    def test_typed_post_is_401(self, client, uploads_dir, upload_type):
        name = "c.yaml" if upload_type == "config" else "i.md"
        response = client.post(
            f"/api/uploads?upload_type={upload_type}", files={"files": (name, b"x")}
        )
        assert response.status_code == 401
        assert not uploads_dir.exists() or not list(uploads_dir.iterdir())

    def test_get_routes_are_401(self, client, uploads_dir, user_a):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        for path in (
            f"/api/uploads/{upload_id}",
            f"/api/uploads/{upload_id}/files",
            f"/api/uploads/{upload_id}/files/doc.txt",
        ):
            response = client.get(path)
            assert response.status_code == 401, path

    def test_delete_is_401_and_touches_nothing(
        self, client, uploads_dir, user_a, rmtree_guard
    ):
        upload_id = _mint()
        upload_dir = _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        response = client.delete(f"/api/uploads/{upload_id}")
        assert response.status_code == 401
        assert upload_dir.exists()
        rmtree_guard.assert_not_called()

    def test_wrong_internal_key_is_401(self, client, uploads_dir, user_a):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        response = client.get(
            f"/api/uploads/{upload_id}", headers={"X-Internal-Key": "not-it"}
        )
        assert response.status_code == 401


# =============================================================================
# Cookie-authenticated cockpit path
# =============================================================================


class TestCookieUser:
    def test_post_creates_upload_bound_to_the_user(self, client, uploads_dir, user_a):
        response = client.post(
            "/api/uploads",
            files=[("files", ("notes.txt", b"hello", "text/plain"))],
            headers={**_cookie("sess-a"), "X-CSRF": "1"},
        )
        assert response.status_code == 201, response.text
        upload_id = response.json()["upload_id"]
        assert orchestrator.uploads._UPLOAD_ID_RE.fullmatch(upload_id)
        metadata = json.loads((uploads_dir / upload_id / "metadata.json").read_text())
        assert metadata["user_id"] == str(user_a["id"])
        assert metadata["files"] == [
            {"name": "notes.txt", "size": 5, "mime_type": "text/plain"}
        ]
        assert (uploads_dir / upload_id / "notes.txt").read_bytes() == b"hello"

    def test_typed_post_as_config(self, client, uploads_dir, user_a):
        response = client.post(
            "/api/uploads?upload_type=config",
            files=[("files", ("agent.yaml", b"llm: {}", "application/yaml"))],
            headers=_cookie("sess-a"),
        )
        assert response.status_code == 201, response.text
        upload_id = response.json()["upload_id"]
        assert upload_id.startswith("config_")
        metadata = json.loads((uploads_dir / upload_id / "metadata.json").read_text())
        assert metadata["user_id"] == str(user_a["id"])

    def test_owner_reads_own_upload(self, client, uploads_dir, user_a):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        info = client.get(f"/api/uploads/{upload_id}", headers=_cookie("sess-a"))
        assert info.status_code == 200
        assert info.json()["upload_id"] == upload_id
        assert "user_id" not in info.json()
        listing = client.get(
            f"/api/uploads/{upload_id}/files", headers=_cookie("sess-a")
        )
        assert listing.status_code == 200
        assert [f["name"] for f in listing.json()] == ["doc.txt"]
        download = client.get(
            f"/api/uploads/{upload_id}/files/doc.txt", headers=_cookie("sess-a")
        )
        assert download.status_code == 200
        assert download.content == b"hello"

    def test_another_user_is_403_on_read_and_delete(
        self, client, uploads_dir, user_a, rmtree_guard
    ):
        upload_id = _mint()
        upload_dir = _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        for path in (
            f"/api/uploads/{upload_id}",
            f"/api/uploads/{upload_id}/files",
            f"/api/uploads/{upload_id}/files/doc.txt",
        ):
            response = client.get(path, headers=_cookie("sess-b"))
            assert response.status_code == 403, path
        response = client.delete(f"/api/uploads/{upload_id}", headers=_cookie("sess-b"))
        assert response.status_code == 403
        assert upload_dir.exists()
        rmtree_guard.assert_not_called()

    def test_admin_reaches_any_upload(self, client, uploads_dir, user_a):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        response = client.get(
            f"/api/uploads/{upload_id}", headers=_cookie("sess-admin")
        )
        assert response.status_code == 200

    def test_owner_deletes_own_upload(self, client, uploads_dir, user_a):
        upload_id = _mint()
        upload_dir = _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        response = client.delete(f"/api/uploads/{upload_id}", headers=_cookie("sess-a"))
        assert response.status_code == 204
        assert not upload_dir.exists()

    def test_legacy_upload_without_owner_is_admin_only(
        self, client, uploads_dir, rmtree_guard
    ):
        """Pre-binding uploads carry no ``user_id``: nobody can prove they own
        them, so regular users are refused and admins/internal callers pass."""
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=None)
        assert (
            client.get(
                f"/api/uploads/{upload_id}", headers=_cookie("sess-a")
            ).status_code
            == 403
        )
        assert (
            client.delete(
                f"/api/uploads/{upload_id}", headers=_cookie("sess-a")
            ).status_code
            == 403
        )
        rmtree_guard.assert_not_called()
        assert (
            client.get(
                f"/api/uploads/{upload_id}", headers=_cookie("sess-admin")
            ).status_code
            == 200
        )
        assert (
            client.get(f"/api/uploads/{upload_id}", headers=_internal()).status_code
            == 200
        )


# =============================================================================
# Internal-key path (the agent runtime)
# =============================================================================


class TestInternalCaller:
    def test_internal_post_records_internal_uploader(self, client, uploads_dir):
        response = client.post(
            "/api/uploads",
            files=[("files", ("notes.txt", b"hello", "text/plain"))],
            headers=_internal(),
        )
        assert response.status_code == 201, response.text
        upload_id = response.json()["upload_id"]
        metadata = json.loads((uploads_dir / upload_id / "metadata.json").read_text())
        assert metadata["user_id"] == orchestrator.uploads.INTERNAL_UPLOADER

    def test_internal_key_reads_any_users_upload(self, client, uploads_dir, user_a):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        assert (
            client.get(f"/api/uploads/{upload_id}", headers=_internal()).status_code
            == 200
        )
        download = client.get(
            f"/api/uploads/{upload_id}/files/doc.txt", headers=_internal()
        )
        assert download.status_code == 200
        assert download.content == b"hello"

    @pytest.mark.asyncio
    async def test_agent_client_downloads_with_internal_key_only(
        self, app, uploads_dir, user_a, monkeypatch
    ):
        """The agent fetches job inputs through ``OrchestratorClient`` with
        ``MCP_INTERNAL_KEY`` and no ``user_id`` (``src/agent.py``); that exact
        header set has to keep working against the gated router."""
        from agent.api.orchestrator_client import OrchestratorClient

        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        seen_headers: list[dict[bytes, bytes]] = []

        async def recording_app(scope, receive, send):
            if scope["type"] == "http":
                seen_headers.append(dict(scope["headers"]))
            await app(scope, receive, send)

        real_async_client = httpx.AsyncClient

        def routed_client(**kwargs):
            return real_async_client(
                transport=httpx.ASGITransport(app=recording_app), **kwargs
            )

        monkeypatch.setattr(httpx, "AsyncClient", routed_client)
        monkeypatch.setenv("MCP_INTERNAL_KEY", INTERNAL_KEY)
        client = OrchestratorClient(
            orchestrator_url="http://testserver",
            pod_ip="",
            pod_port=0,
            hostname="",
            config_name="",
        )
        try:
            await client.connect()
            info = await client.get_upload_info(upload_id)
            assert info is not None
            assert [f.name for f in info.files] == ["doc.txt"]
            assert await client.download_file(upload_id, "doc.txt") == b"hello"
        finally:
            await client.close()

        assert seen_headers, "no request reached the router"
        for headers in seen_headers:
            assert headers.get(b"x-internal-key") == INTERNAL_KEY.encode()
            assert b"x-mcp-user-id" not in headers


# =============================================================================
# upload_id validation — nothing reaches the filesystem on a bad id
# =============================================================================


class TestUploadIdValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("path", ["/api/uploads/..", "/api/uploads/../x"])
    async def test_delete_dot_segments_never_reach_rmtree(
        self, app, uploads_dir, rmtree_guard, path
    ):
        status, _ = await _raw_asgi(app, "DELETE", path, _internal())
        assert status in (400, 404)
        rmtree_guard.assert_not_called()

    def test_delete_percent_encoded_dot_segment_is_rejected(
        self, client, uploads_dir, rmtree_guard
    ):
        response = client.delete("/api/uploads/%2e%2e", headers=_internal())
        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid upload id"
        rmtree_guard.assert_not_called()

    @pytest.mark.parametrize(
        "bad_id",
        [
            "..",
            "documents_1756800000000_0123456789abcdeg",  # not hex
            "documents_1756800000000_0123456789abcde",  # 15 hex chars
            "documents_175680000000_0123456789abcdef",  # 12 digits
            "other_1756800000000_0123456789abcdef",  # unknown type
            "documents_1756800000000_0123456789ABCDEF",  # upper-case hex
            "metadata.json",
        ],
    )
    def test_malformed_ids_are_400_on_every_route(
        self, client, uploads_dir, rmtree_guard, bad_id
    ):
        for method, path in (
            ("GET", f"/api/uploads/{bad_id}"),
            ("GET", f"/api/uploads/{bad_id}/files"),
            ("GET", f"/api/uploads/{bad_id}/files/doc.txt"),
            ("DELETE", f"/api/uploads/{bad_id}"),
        ):
            response = client.request(method, path, headers=_internal())
            assert response.status_code in (400, 404), (method, path)
            if response.status_code == 400:
                assert response.json()["detail"] == "Invalid upload id"
        rmtree_guard.assert_not_called()

    def test_well_formed_unknown_id_is_404_without_rmtree(
        self, client, uploads_dir, rmtree_guard
    ):
        response = client.delete(f"/api/uploads/{_mint()}", headers=_internal())
        assert response.status_code == 404
        rmtree_guard.assert_not_called()

    def test_minted_ids_match_the_pin(self, client, uploads_dir):
        response = client.post(
            "/api/uploads",
            files=[("files", ("a.txt", b"x", "text/plain"))],
            headers=_internal(),
        )
        assert response.status_code == 201
        assert orchestrator.uploads._validate_upload_id(response.json()["upload_id"])

    @pytest.mark.asyncio
    async def test_filename_traversal_still_guarded(
        self, uploads_dir, internal_key, user_a
    ):
        """The pre-existing filename guard (``_sanitize_filename`` + the
        ``relative_to`` check) is kept; a traversal filename never leaves
        the upload directory. Called directly: the ASGI server decodes
        ``%2F`` before routing, so over HTTP such a path never matches."""
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        (uploads_dir / "secret.txt").write_text("outside")
        request = MagicMock()
        request.headers = _internal()
        with pytest.raises(HTTPException) as exc:
            await orchestrator.uploads.get_uploaded_file(
                request, upload_id, "../secret.txt"
            )
        assert exc.value.status_code == 404


# =============================================================================
# Size limit is enforced while streaming
# =============================================================================


class _ChunkedUpload:
    """Stands in for starlette's UploadFile: serves ``total`` bytes on demand
    and counts how much the handler actually pulled."""

    filename = "big.bin"
    content_type = "application/octet-stream"
    size = None

    def __init__(self, total: int):
        self._remaining = total
        self.served = 0

    async def read(self, size: int = -1) -> bytes:
        n = self._remaining if size is None or size < 0 else min(size, self._remaining)
        self._remaining -= n
        self.served += n
        return b"x" * n


class TestSizeLimit:
    def test_limit_value_unchanged_and_label_matches(self):
        assert orchestrator.uploads.MAX_FILE_SIZE == 5 * 1024 * 1024 * 1024
        assert orchestrator.uploads._MAX_FILE_SIZE_LABEL == "5120 MB"
        assert "50MB" not in (orchestrator.uploads.upload_files.__doc__ or "")

    @pytest.mark.asyncio
    async def test_oversized_upload_is_refused_mid_stream(
        self, uploads_dir, internal_key, monkeypatch
    ):
        monkeypatch.setattr(orchestrator.uploads, "MAX_FILE_SIZE", 1_000)
        monkeypatch.setattr(orchestrator.uploads, "UPLOAD_CHUNK_SIZE", 100)
        upload = _ChunkedUpload(total=100_000)
        request = MagicMock()
        request.headers = _internal()

        with pytest.raises(HTTPException) as exc:
            await orchestrator.uploads.upload_files(
                request,
                files=[upload],
                upload_type=orchestrator.uploads.UploadType.DOCUMENTS,
            )

        assert exc.value.status_code == 413
        assert "exceeds maximum size" in exc.value.detail
        # Stopped at the first chunk past the limit — never buffered the body.
        assert upload.served <= 1_000 + 100
        assert upload.served < 100_000
        # The partial upload directory is gone.
        assert not uploads_dir.exists() or not list(uploads_dir.iterdir())

    def test_oversized_multipart_is_413_and_cleaned_up(
        self, client, uploads_dir, monkeypatch
    ):
        monkeypatch.setattr(orchestrator.uploads, "MAX_FILE_SIZE", 1_000)
        monkeypatch.setattr(orchestrator.uploads, "UPLOAD_CHUNK_SIZE", 100)
        response = client.post(
            "/api/uploads",
            files=[("files", ("big.bin", b"x" * 5_000, "application/octet-stream"))],
            headers=_internal(),
        )
        assert response.status_code == 413
        assert not uploads_dir.exists() or not list(uploads_dir.iterdir())


# =============================================================================
# POST /api/jobs binds referenced uploads to the calling user
# =============================================================================


def _create_job_as(fake_db, caller) -> ExitStack:
    """Cockpit-path ``create_job`` with the caller resolved and the LLM
    readiness gate out of the way — the shape ``tests/test_internal_auth.py``
    uses for the same handler."""
    stack = ExitStack()
    stack.enter_context(patch.object(access_module, "_INTERNAL_KEY", INTERNAL_KEY))
    stack.enter_context(
        patch("orchestrator.main.app.state.resources.postgres_db", fake_db)
    )
    stack.enter_context(
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=caller),
        )
    )
    stack.enter_context(
        patch(
            "orchestrator.application.access.enforce_readiness_gate",
            AsyncMock(return_value=None),
        )
    )
    return stack


class TestCreateJobUploadOwnership:
    @pytest.mark.asyncio
    async def test_another_users_upload_is_403(
        self, uploads_dir, user_a, user_b, fake_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        fake_request.headers = {}
        fake_request.cookies = {}
        body = JobCreate(description="use someone else's files", upload_id=upload_id)

        with _create_job_as(fake_db, user_b), pytest.raises(HTTPException) as exc:
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "key", ["upload_id", "config_upload_id", "instructions_upload_id"]
    )
    async def test_upload_ids_smuggled_through_context_are_403(
        self, uploads_dir, user_a, user_b, fake_db, fake_request, key
    ):
        """The dispatcher reads the ids from ``jobs.context``; a body that
        sets them there directly must meet the same check."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        upload_id = _mint("config" if key == "config_upload_id" else "documents")
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        fake_request.headers = {}
        fake_request.cookies = {}
        body = JobCreate(description="smuggled", context={key: upload_id})

        with _create_job_as(fake_db, user_b), pytest.raises(HTTPException) as exc:
            await create_job(fake_request, body)

        assert exc.value.status_code == 403
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_and_missing_upload_ids_are_refused(
        self, uploads_dir, user_a, fake_db, fake_request
    ):
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        fake_request.headers = {}
        fake_request.cookies = {}
        for upload_id, expected in (("../../etc", 400), (_mint(), 404)):
            body = JobCreate(description="x", config_upload_id=upload_id)
            with _create_job_as(fake_db, user_a), pytest.raises(HTTPException) as exc:
                await create_job(fake_request, body)
            assert exc.value.status_code == expected, upload_id
        fake_db.create_job.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_owner_passes_the_upload_check(
        self, uploads_dir, user_a, fake_db, fake_request
    ):
        """Sentinel on the step right after the ownership check: reaching it
        proves the owner's own upload was accepted. An HTTPException subclass
        so ``create_job``'s ``except Exception`` → 500 wrapper lets it out."""
        from orchestrator.schemas.job_create import JobCreate
        from tests._b09_control_seams import create_job

        class _PastTheUploadCheck(HTTPException):
            pass

        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        fake_request.headers = {}
        fake_request.cookies = {}
        fake_db.get_user = AsyncMock(return_value=None)
        body = JobCreate(description="own files", upload_id=upload_id)

        with (
            _create_job_as(fake_db, user_a),
            patch(
                "orchestrator.application.jobs.require_job_project_access",
                AsyncMock(
                    side_effect=_PastTheUploadCheck(status_code=599, detail="past")
                ),
            ),
            pytest.raises(_PastTheUploadCheck),
        ):
            await create_job(fake_request, body)
        fake_db.create_job.assert_not_awaited()


class TestAuthorizeUploadReference:
    """The helper ``create_job`` calls, over every principal shape it sees."""

    def test_owner_admin_internal_pass(self, uploads_dir, user_a, user_admin):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        assert orchestrator.uploads.authorize_upload_reference(user_a, upload_id)[
            "upload_id"
        ] == (upload_id)
        assert orchestrator.uploads.authorize_upload_reference(user_admin, upload_id)
        assert orchestrator.uploads.authorize_upload_reference(
            None, upload_id, internal=True
        )

    def test_other_user_and_no_principal_are_403(self, uploads_dir, user_a, user_b):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        with pytest.raises(HTTPException) as exc:
            orchestrator.uploads.authorize_upload_reference(user_b, upload_id)
        assert exc.value.status_code == 403
        with pytest.raises(HTTPException) as exc:
            orchestrator.uploads.authorize_upload_reference(None, upload_id)
        assert exc.value.status_code == 403

    def test_legacy_ownerless_upload_is_admin_or_internal_only(
        self, uploads_dir, user_a, user_admin
    ):
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=None)
        with pytest.raises(HTTPException) as exc:
            orchestrator.uploads.authorize_upload_reference(user_a, upload_id)
        assert exc.value.status_code == 403
        assert orchestrator.uploads.authorize_upload_reference(user_admin, upload_id)
        assert orchestrator.uploads.authorize_upload_reference(
            None, upload_id, internal=True
        )

    def test_internal_upload_is_not_a_users(self, uploads_dir, user_a):
        upload_id = _mint()
        _write_upload(
            uploads_dir, upload_id, owner=orchestrator.uploads.INTERNAL_UPLOADER
        )
        with pytest.raises(HTTPException) as exc:
            orchestrator.uploads.authorize_upload_reference(user_a, upload_id)
        assert exc.value.status_code == 403

    def test_view_as_user_shadow_narrows_admin(self, uploads_dir, user_a, user_admin):
        """``X-Admin-View-As: user`` sets ``is_admin=False`` on the resolved
        dict; the upload check honours the shadow like other visibility."""
        upload_id = _mint()
        _write_upload(uploads_dir, upload_id, owner=str(user_a["id"]))
        shadowed = {**user_admin, "is_admin": False, "real_is_admin": True}
        with pytest.raises(HTTPException) as exc:
            orchestrator.uploads.authorize_upload_reference(shadowed, upload_id)
        assert exc.value.status_code == 403
