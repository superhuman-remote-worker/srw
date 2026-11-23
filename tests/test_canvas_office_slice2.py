"""Canvas Office Documents Slice 2: binary editing and WOPI write contracts."""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.services.canvas import (
    CanvasEditError,
    CanvasMutation,
    CanvasRecord,
    CanvasService,
    CanvasSetInput,
    WorkspaceFileSource,
    canonical_source_fingerprint,
)
from orchestrator.services.canvas_files import (
    CanvasFileError,
    RawWorkspaceFile,
    ThreadWorkspaceFileGateway,
    ValidatedCanvasFile,
)
from orchestrator.services.canvas_office import (
    CanvasOfficeError,
    CollaboraConfig,
    WopiAccess,
    WopiTokenService,
    wopi_file_id,
)

THREAD_ID = "a3333333-3333-3333-3333-333333333333"
USER_ID = "b4444444-4444-4444-8444-444444444444"
GENERATION = UUID("11111111-aaaa-4aaa-8aaa-111111111111")
PATH = "output/quarterly report.docx"
NOW = datetime(2026, 7, 25, 12, 30, 45, 123456, tzinfo=timezone.utc)
NEW_NOW = NOW + timedelta(seconds=1, microseconds=111111)
OLD_BYTES = b"PK\x03\x04office-document-v1"
NEW_BYTES = b"PK\x03\x04office-document-v2"
OLD_VERSION = "sha256:" + hashlib.sha256(OLD_BYTES).hexdigest()
NEW_VERSION = "sha256:" + hashlib.sha256(NEW_BYTES).hexdigest()
FILE_ID = wopi_file_id(THREAD_ID, PATH)


class _OfficeMagic:
    @staticmethod
    def from_buffer(data: bytes, mime: bool = False) -> str:
        del mime
        if data.startswith(b"PK\x03\x04"):
            return "application/zip"
        return "text/plain"


def _config(*, enabled: bool = True) -> CollaboraConfig:
    return CollaboraConfig(
        enabled=enabled,
        internal_url="http://srw-collabora:9980",
        public_origin="https://office.example.test",
        wopi_base_url="http://srw-orchestrator:8085",
        cockpit_origin="https://cockpit.example.test",
        token_ttl_seconds=36_000,
        discovery_cache_ttl_seconds=60,
        request_timeout_seconds=2.0,
    )


def _thread(*, backend: str = "sandbox") -> dict[str, Any]:
    return {
        "id": THREAD_ID,
        "user_id": USER_ID,
        "metadata": {
            "config_override": {"workspace": {"backend": backend}},
            "_workspace_binding": {
                "generation": str(GENERATION),
                "kind": "virtual" if backend == "virtual" else "remote",
                "backing_id": "test-backing",
                "ssh_host_key_fingerprint": (
                    None if backend == "virtual" else "SHA256:test"
                ),
            },
            "workspace_container": {
                "status": "ready",
                "host": "workspace.test",
                "port": 22,
                "_canvas_workspace_generation": str(GENERATION),
            },
        },
    }


def _user() -> dict[str, Any]:
    return {
        "id": USER_ID,
        "display_name": "Ada Lovelace",
        "is_admin": False,
        "is_approved": True,
    }


def _record(
    *,
    editable: bool = True,
    revision: int = 4,
    source_version: str = OLD_VERSION,
    updated_at: datetime = NOW,
) -> CanvasRecord:
    source = WorkspaceFileSource(path=PATH, workspace_generation=GENERATION)
    return CanvasRecord(
        thread_id=THREAD_ID,
        canvas_id="main",
        source=source,
        title="Quarterly report",
        renderer="office",
        editable=editable,
        alt_text=None,
        presentation_revision=revision,
        source_fingerprint=canonical_source_fingerprint(source),
        source_version=source_version,
        origin_generation=None,
        created_at=NOW,
        updated_at=updated_at,
    )


def _file(
    data: bytes = OLD_BYTES,
    *,
    last_modified: datetime = NOW,
) -> ValidatedCanvasFile:
    return ValidatedCanvasFile(
        path=PATH,
        data=data,
        media_type=(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ),
        renderer="office",
        source_version="sha256:" + hashlib.sha256(data).hexdigest(),
        last_modified=last_modified,
    )


@pytest.mark.asyncio
async def test_write_tokens_can_read_and_write_but_read_tokens_cannot_putfile() -> None:
    state: dict[str, Any] = {
        "record": _record(),
        "thread": _thread(),
        "writable": True,
    }

    async def user_loader(user_id: str):
        return _user() if user_id == USER_ID else None

    async def thread_loader(thread_id: str):
        return state["thread"] if thread_id == THREAD_ID else None

    async def canvas_loader(thread_id: str):
        return state["record"] if thread_id == THREAD_ID else None

    service = WopiTokenService(
        "office-token-secret-with-at-least-32-bytes",
        ttl_seconds=600,
        user_loader=user_loader,
        thread_loader=thread_loader,
        canvas_loader=canvas_loader,
        editing_checker=lambda thread, record: bool(state["writable"]),
        clock=lambda: 1_721_822_400,
    )
    write_grant = service.mint(
        user_id=USER_ID,
        thread_id=THREAD_ID,
        path=PATH,
        write_flag=True,
    )

    # CheckFileInfo/GetFile remain legal for the same write-scoped session.
    assert (
        await service.authenticate(
            write_grant.access_token,
            file_id=write_grant.file_id,
            require_write=False,
        )
    ).claims["write_flag"] is True
    assert (
        await service.authenticate(
            write_grant.access_token,
            file_id=write_grant.file_id,
            require_write=True,
        )
    ).record.editable is True

    state["record"] = _record(editable=False)
    read_grant = service.mint(
        user_id=USER_ID,
        thread_id=THREAD_ID,
        path=PATH,
        write_flag=False,
    )
    with pytest.raises(CanvasOfficeError) as read_only:
        await service.authenticate(
            read_grant.access_token,
            file_id=read_grant.file_id,
            require_write=True,
        )
    assert read_only.value.status_code == 403
    assert read_only.value.code == "wopi_access_denied"

    state["record"] = _record()
    state["writable"] = False
    with pytest.raises(CanvasOfficeError) as unwritable:
        await service.authenticate(
            write_grant.access_token,
            file_id=write_grant.file_id,
            require_write=True,
        )
    assert unwritable.value.code == "wopi_access_denied"


class _RouteTokens:
    def __init__(self, *, write_flag: bool = True) -> None:
        self.write_flag = write_flag
        self.calls: list[bool] = []

    async def authenticate(self, token: str, *, file_id: str, require_write: bool):
        assert token == "wopi-token"
        assert file_id == FILE_ID
        self.calls.append(require_write)
        if require_write and not self.write_flag:
            raise CanvasOfficeError(
                403,
                "wopi_access_denied",
                "WOPI token scope does not permit this call",
            )
        record = _record(editable=self.write_flag)
        return WopiAccess(
            user=_user(),
            thread=_thread(),
            record=record,
            claims={
                "sub": USER_ID,
                "tid": THREAD_ID,
                "path": PATH,
                "write_flag": self.write_flag,
            },
        )


class _RouteGateway:
    def __init__(self) -> None:
        self.file = _file()
        self.writes: list[bytes] = []
        self.validated: list[bytes] = []

    async def materialize_binary(self, thread, record):
        assert thread == _thread()
        assert record.renderer == "office"
        return self.file

    def supports_editing(self, thread, record=None):
        assert thread == _thread()
        return bool(record and record.renderer == "office" and record.editable)

    async def validate_edit_candidate(self, record, data):
        assert record.renderer == "office"
        self.validated.append(data)
        return _file(data)

    async def replace_current_binary(
        self,
        thread,
        record,
        data,
        *,
        expected_source_version,
    ):
        assert thread["id"] == THREAD_ID
        assert record.renderer == "office"
        assert expected_source_version == OLD_VERSION
        self.writes.append(data)
        self.file = _file(data, last_modified=NEW_NOW)
        return self.file


class _RouteCanvasService:
    def __init__(self, gateway: _RouteGateway) -> None:
        self.gateway = gateway
        self.edit_calls = 0

    async def edit_file(
        self,
        thread_id,
        *,
        expected_presentation_revision,
        expected_source_fingerprint,
        expected_source_version,
        expected_thread_user_id,
        writer,
        **kwargs,
    ):
        del kwargs
        self.edit_calls += 1
        record = _record()
        assert thread_id == THREAD_ID
        assert expected_presentation_revision == record.presentation_revision
        assert expected_source_fingerprint == record.source_fingerprint
        assert expected_source_version == OLD_VERSION
        assert expected_thread_user_id == USER_ID
        version = await writer(record, _thread())
        assert version == NEW_VERSION
        return CanvasMutation(
            changed=True,
            record=replace(
                record,
                presentation_revision=record.presentation_revision + 1,
                source_version=version,
                updated_at=NEW_NOW,
            ),
        )


def _wopi_client(monkeypatch, *, write_flag: bool = True):
    from orchestrator.routers import wopi

    tokens = _RouteTokens(write_flag=write_flag)
    gateway = _RouteGateway()
    service = _RouteCanvasService(gateway)
    monkeypatch.setattr(wopi, "_get_token_service", lambda *args, **kwargs: tokens)
    monkeypatch.setattr(wopi, "_get_file_gateway", lambda *args, **kwargs: gateway)
    monkeypatch.setattr(wopi, "_get_canvas_service", lambda *args, **kwargs: service)
    monkeypatch.setattr(wopi, "_get_collabora_config", _config)
    app = FastAPI()
    app.state.store = object()
    app.include_router(wopi.router)
    return TestClient(app), tokens, gateway, service


def _putfile(
    client: TestClient,
    *,
    timestamp: str | None | object = "2026-07-25T12:30:45.123456Z",
    content: bytes = NEW_BYTES,
):
    headers = {
        "X-WOPI-Override": "PUT",
        "X-COOL-WOPI-IsAutosave": "true",
        "X-COOL-WOPI-IsExitSave": "false",
        "X-COOL-WOPI-IsModifiedByUser": "true",
    }
    if isinstance(timestamp, str):
        headers["X-COOL-WOPI-Timestamp"] = timestamp
    return client.post(
        f"/wopi/files/{FILE_ID}/contents?access_token=wopi-token",
        headers=headers,
        content=content,
    )


def test_check_file_info_advertises_write_only_for_write_scoped_session(
    monkeypatch,
) -> None:
    client, _, _, _ = _wopi_client(monkeypatch)
    response = client.get(f"/wopi/files/{FILE_ID}?access_token=wopi-token")
    assert response.status_code == 200
    assert response.json()["UserCanWrite"] is True

    read_client, _, _, _ = _wopi_client(monkeypatch, write_flag=False)
    read_response = read_client.get(f"/wopi/files/{FILE_ID}?access_token=wopi-token")
    assert read_response.status_code == 200
    assert "UserCanWrite" not in read_response.json()


def test_putfile_timestamp_mismatch_returns_409_1010_without_write(monkeypatch) -> None:
    client, tokens, gateway, service = _wopi_client(monkeypatch)
    response = _putfile(
        client,
        timestamp="2026-07-25T12:30:45.123455Z",
    )

    assert response.status_code == 409
    assert response.json() == {"COOLStatusCode": 1010}
    assert gateway.writes == []
    assert service.edit_calls == 0
    assert True in tokens.calls


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-07-25T12:30:45.123456Z",
        None,  # Collabora's explicit forced-overwrite contract.
    ],
)
def test_putfile_matching_or_omitted_timestamp_saves_and_echoes_new_mtime(
    monkeypatch,
    timestamp,
) -> None:
    client, _, gateway, service = _wopi_client(monkeypatch)
    response = _putfile(client, timestamp=timestamp)

    assert response.status_code == 200
    assert response.json() == {"LastModifiedTime": "2026-07-25T12:30:46.234567Z"}
    assert gateway.validated == [NEW_BYTES]
    assert gateway.writes == [NEW_BYTES]
    assert service.edit_calls == 1


def test_putfile_rejects_read_token_and_office_oversize_before_write(
    monkeypatch,
) -> None:
    client, _, gateway, service = _wopi_client(monkeypatch, write_flag=False)
    denied = _putfile(client)
    assert denied.status_code == 403
    assert gateway.writes == []
    assert service.edit_calls == 0

    from orchestrator.routers import wopi

    write_client, _, write_gateway, write_service = _wopi_client(monkeypatch)
    monkeypatch.setattr(wopi, "MAX_OFFICE_BYTES", 16)
    oversized = _putfile(write_client, content=b"x" * 17)
    assert oversized.status_code == 413
    assert write_gateway.writes == []
    assert write_service.edit_calls == 0


@pytest.mark.asyncio
async def test_binary_gateway_rechecks_hash_writes_and_magic_validates_readback(
    monkeypatch,
) -> None:
    from orchestrator.services import canvas_files

    monkeypatch.setattr(canvas_files, "magic", _OfficeMagic)
    storage = {PATH: RawWorkspaceFile(OLD_BYTES, NOW)}
    writes: list[bytes] = []

    async def load(thread, path, generation):
        del thread
        assert generation == GENERATION
        return storage[path]

    async def write(thread, path, generation, data):
        del thread
        assert generation == GENERATION
        writes.append(data)
        storage[path] = RawWorkspaceFile(data, NEW_NOW)

    gateway = ThreadWorkspaceFileGateway(
        remote_loader=load,
        remote_writer=write,
    )
    saved = await gateway.replace_current_binary(
        _thread(),
        _record(),
        NEW_BYTES,
        expected_source_version=OLD_VERSION,
    )
    assert writes == [NEW_BYTES]
    assert saved.source_version == NEW_VERSION
    assert saved.last_modified == NEW_NOW

    storage[PATH] = RawWorkspaceFile(b"PK\x03\x04external-writer", NEW_NOW)
    with pytest.raises(CanvasFileError) as stale:
        await gateway.replace_current_binary(
            _thread(),
            _record(),
            b"PK\x03\x04must-not-land",
            expected_source_version=OLD_VERSION,
        )
    assert stale.value.status_code == 412
    assert stale.value.code == "canvas_content_precondition_failed"
    assert writes == [NEW_BYTES]

    with pytest.raises(CanvasFileError) as mismatch:
        await gateway.validate_edit_candidate(_record(), b"plain text")
    assert mismatch.value.code in {"mime_renderer_mismatch", "unsupported_canvas_file"}


class _OfficeEditDB:
    """Asyncpg-shaped row store proving the existing coordinator is reused."""

    def __init__(self) -> None:
        record = _record()
        self.row = {
            **{
                name: getattr(record, name)
                for name in CanvasRecord.__dataclass_fields__
            },
            "source": json.dumps(record.source.model_dump(mode="json")),
        }
        self.thread = _thread()
        self.row_lock = asyncio.Lock()
        self.advisory_owner: _OfficeEditConnection | None = None
        self.now = NOW

    @asynccontextmanager
    async def acquire(self):
        connection = _OfficeEditConnection(self)
        try:
            yield connection
        finally:
            assert self.advisory_owner is not connection


class _OfficeEditConnection:
    def __init__(self, db: _OfficeEditDB) -> None:
        self.db = db

    @asynccontextmanager
    async def transaction(self):
        async with self.db.row_lock:
            yield

    async def fetchval(self, query: str, key: int) -> bool:
        del key
        if "pg_try_advisory_lock" in query:
            if self.db.advisory_owner in {None, self}:
                self.db.advisory_owner = self
                return True
            return False
        if "pg_advisory_unlock" in query:
            if self.db.advisory_owner is self:
                self.db.advisory_owner = None
                return True
            return False
        raise AssertionError(query)

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        normalized = " ".join(query.split())
        if "FROM threads" in normalized:
            return {
                "id": THREAD_ID,
                "user_id": USER_ID,
                "metadata": self.db.thread["metadata"],
            }
        if normalized.startswith("SELECT thread_id"):
            return dict(self.db.row)
        if normalized.startswith("UPDATE canvases SET source_version"):
            self.db.now += timedelta(microseconds=1)
            self.db.row.update(
                source_version=args[2],
                presentation_revision=self.db.row["presentation_revision"] + 1,
                updated_at=self.db.now,
            )
            return dict(self.db.row)
        raise AssertionError(normalized)


@pytest.mark.asyncio
async def test_office_writer_uses_edit_file_serialization_and_bumps_revision() -> None:
    db = _OfficeEditDB()
    service = CanvasService(db)
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_called = False
    record = _record()
    assert record.source_fingerprint is not None

    async def first_writer(current, thread):
        assert current.renderer == "office"
        assert thread["user_id"] == USER_ID
        first_started.set()
        await release_first.wait()
        return NEW_VERSION

    async def second_writer(current, thread):
        nonlocal second_called
        del current, thread
        second_called = True
        return "sha256:" + "c" * 64

    first = asyncio.create_task(
        service.edit_file(
            THREAD_ID,
            expected_presentation_revision=record.presentation_revision,
            expected_source_fingerprint=record.source_fingerprint,
            expected_source_version=OLD_VERSION,
            expected_thread_user_id=USER_ID,
            writer=first_writer,
        )
    )
    await asyncio.wait_for(first_started.wait(), timeout=1)
    second = asyncio.create_task(
        service.edit_file(
            THREAD_ID,
            expected_presentation_revision=record.presentation_revision,
            expected_source_fingerprint=record.source_fingerprint,
            expected_source_version=OLD_VERSION,
            expected_thread_user_id=USER_ID,
            writer=second_writer,
            lock_timeout=1,
        )
    )
    await asyncio.sleep(0.06)
    assert not second.done()

    release_first.set()
    mutation = await first
    assert mutation.record is not None
    assert mutation.record.presentation_revision == 5
    assert mutation.record.source_version == NEW_VERSION
    with pytest.raises(CanvasEditError) as conflict:
        await second
    assert conflict.value.code == "canvas_presentation_changed"
    assert second_called is False
    assert db.advisory_owner is None


@pytest.mark.asyncio
async def test_four_editable_gates_agree_for_writable_office_and_reject_memory(
    monkeypatch,
) -> None:
    from orchestrator.services import canvas_files

    monkeypatch.setattr(canvas_files, "magic", _OfficeMagic)
    record = _record()
    assert record.source_fingerprint is not None

    # Gate 1: the durable set model admits the structurally valid Office mode.
    presentation = CanvasSetInput(
        source=record.source,
        title=record.title,
        renderer="office",
        editable=True,
        source_version=record.source_version,
    )
    assert presentation.editable is True

    # Gate 2: the unchanged coordinator accepts Office and reaches its writer.
    validated_source = CanvasService._validate_edit_record(
        record,
        expected_presentation_revision=record.presentation_revision,
        expected_source_fingerprint=record.source_fingerprint,
        expected_source_version=record.source_version,
    )
    assert validated_source.path == PATH

    # Gate 3: a writable adapter both advertises editing and magic-re-sniffs.
    gateway = ThreadWorkspaceFileGateway(
        remote_writer=lambda *args: asyncio.sleep(0),
    )
    assert gateway.supports_editing(_thread(), record) is True
    candidate = await gateway.validate_edit_candidate(record, NEW_BYTES)
    assert candidate.renderer == "office"

    # Gate 4's backend predicate fails closed for the virtual memory tier.
    monkeypatch.setattr(
        canvas_files,
        "virtual_workspace_rclone_spec",
        lambda: {"type": "memory", "config": {}, "root": ""},
    )
    monkeypatch.setattr(canvas_files.shutil, "which", lambda name: f"/usr/bin/{name}")
    assert gateway.supports_editing(_thread(backend="virtual"), record) is False


def test_putfile_runs_inside_the_vm_write_authority(monkeypatch) -> None:
    """A VM Office save holds the durable VM write lease around edit_file.

    The authority is a no-op for this container thread; the wrapper is what a
    VM Session's save goes through (tests/test_canvas_vm_workspace.py).
    """

    from contextlib import asynccontextmanager

    from orchestrator.routers import wopi

    client, _, gateway, service = _wopi_client(monkeypatch)
    entered: list[tuple[str, object]] = []
    inside: list[bool] = []
    original_edit = service.edit_file

    @asynccontextmanager
    async def authority(thread, *, store):
        entered.append((thread["id"], store))
        inside.append(True)
        try:
            yield
        finally:
            inside.append(False)

    async def edit_file(*args, **kwargs):
        assert inside and inside[-1] is True
        return await original_edit(*args, **kwargs)

    monkeypatch.setattr(wopi, "vm_canvas_write_authority", authority)
    monkeypatch.setattr(service, "edit_file", edit_file)

    response = _putfile(client)

    assert response.status_code == 200
    assert entered == [(THREAD_ID, client.app.state.store)]
    assert inside == [True, False]
    assert gateway.writes == [NEW_BYTES]
