"""Dynamic Canvas Slice 1 backend, gateway, and lifecycle contracts."""

from __future__ import annotations

import json
import os
import stat
import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from orchestrator.database.postgres import PostgresDB
from orchestrator.services.canvas import (
    CanvasMutation,
    CanvasPreconditionFailed,
    CanvasRecord,
    WorkspaceFileSource,
    build_public_canvas_representation,
    canonical_source_fingerprint,
)
from orchestrator.services.canvas_files import (
    CanvasFileError,
    RawWorkspaceFile,
    ThreadWorkspaceFileGateway,
    ValidatedCanvasFile,
    canonical_workspace_path,
    validate_canvas_bytes,
)

THREAD_ID = "a3333333-3333-3333-3333-333333333333"
USER_ID = "b4444444-4444-4444-8444-444444444444"
GENERATION = UUID("11111111-aaaa-4aaa-8aaa-111111111111")
NOW = datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc)


def _thread(*, generation: UUID = GENERATION, backend: str = "sandbox") -> dict:
    return {
        "id": THREAD_ID,
        "user_id": USER_ID,
        "metadata": {
            "config_override": {"workspace": {"backend": backend}},
            "_workspace_binding": {
                "generation": str(generation),
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
                "_canvas_workspace_generation": str(generation),
            },
        },
    }


def _file(data: bytes = b"# Canvas\n") -> ValidatedCanvasFile:
    import hashlib

    return ValidatedCanvasFile(
        path="output/report.md",
        data=data,
        media_type="text/markdown",
        renderer="markdown",
        source_version="sha256:" + hashlib.sha256(data).hexdigest(),
        last_modified=NOW,
    )


def _office_file(data: bytes = b"PK\x03\x04office") -> ValidatedCanvasFile:
    import hashlib

    return ValidatedCanvasFile(
        path="output/report.docx",
        data=data,
        media_type=(
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        ),
        renderer="office",
        source_version="sha256:" + hashlib.sha256(data).hexdigest(),
        last_modified=NOW,
    )


def _record(
    data: bytes = b"# Canvas\n", revision: int = 1, *, editable: bool = False
) -> CanvasRecord:
    file = _file(data)
    source = WorkspaceFileSource(
        path=file.path,
        workspace_generation=GENERATION,
    )
    return CanvasRecord(
        thread_id=THREAD_ID,
        canvas_id="main",
        source=source,
        title="Report",
        renderer="markdown",
        editable=editable,
        alt_text=None,
        presentation_revision=revision,
        source_fingerprint=canonical_source_fingerprint(source),
        source_version=file.source_version,
        origin_generation=None,
        created_at=NOW,
        updated_at=NOW,
    )


class _Magic:
    @staticmethod
    def from_buffer(data: bytes, mime: bool = False) -> str:
        del mime
        if data.startswith(b"\x89PNG"):
            return "image/png"
        if data.startswith(b"<"):
            return "text/html"
        return "text/plain"


def _write_streaming_rclone(tmp_path: Path) -> Path:
    executable = tmp_path / "rclone"
    executable.write_text(
        """#!/usr/bin/env python3
import json
import os
import signal
import sys
import time

marker = os.environ.get("RCLONE_CONFIG_SRW_MARKER")
started = os.environ.get("RCLONE_CONFIG_SRW_STARTED")
mode = os.environ.get("RCLONE_CONFIG_SRW_MODE", "overflow")

def stop(signum, frame):
    del signum, frame
    if marker:
        with open(marker, "w", encoding="utf-8") as handle:
            handle.write(str(os.getpid()))
    raise SystemExit(0)

signal.signal(signal.SIGTERM, stop)
if sys.argv[1] == "lsjson":
    print(json.dumps({"Path": "report.md", "Size": 1, "IsDir": False}))
    raise SystemExit(0)
if started:
    with open(started, "w", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))
if mode == "hang":
    while True:
        time.sleep(1)
while True:
    os.write(1, b"x" * 4096)
"""
    )
    executable.chmod(0o755)
    return executable


def test_path_and_byte_validation_fail_closed(monkeypatch) -> None:
    from orchestrator.services import canvas_files

    monkeypatch.setattr(canvas_files, "magic", _Magic)
    assert canonical_workspace_path("output/report.md") == "output/report.md"
    for path in (
        "../secret",
        "/etc/passwd",
        "output//x.md",
        "output\\x.md",
        "output/\x1fx.md",
    ):
        with pytest.raises(CanvasFileError):
            canonical_workspace_path(path)

    markdown = validate_canvas_bytes("output/report.md", b"# Safe\n")
    assert markdown.renderer == "markdown"
    assert markdown.media_type == "text/markdown"
    with pytest.raises(CanvasFileError, match="extension"):
        validate_canvas_bytes("output/not-really.png", b"<script>x</script>")
    with pytest.raises(CanvasFileError, match="incompatible"):
        validate_canvas_bytes(
            "output/report.md", b"# Safe\n", requested_renderer="html"
        )


@pytest.mark.parametrize(
    ("path", "detected", "media_type"),
    [
        (
            "output/report.docx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        (
            "output/budget.xlsx",
            "application/zip",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
        (
            "output/slides.pptx",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        ),
        (
            "output/letter.odt",
            "application/vnd.oasis.opendocument.text",
            "application/vnd.oasis.opendocument.text",
        ),
        (
            "output/table.ods",
            "application/vnd.oasis.opendocument.spreadsheet",
            "application/vnd.oasis.opendocument.spreadsheet",
        ),
        (
            "output/deck.odp",
            "application/zip",
            "application/vnd.oasis.opendocument.presentation",
        ),
    ],
)
def test_office_detection_requires_matching_extension_and_magic(
    monkeypatch, path: str, detected: str, media_type: str
) -> None:
    from orchestrator.services import canvas_files

    class OfficeMagic:
        @staticmethod
        def from_buffer(data: bytes, mime: bool = False) -> str:
            del data, mime
            return detected

    monkeypatch.setattr(canvas_files, "magic", OfficeMagic)
    validated = validate_canvas_bytes(path, b"PK\x03\x04office bytes")

    assert validated.renderer == "office"
    assert validated.media_type == media_type
    assert validated.data == b"PK\x03\x04office bytes"


@pytest.mark.parametrize(
    ("path", "detected"),
    [
        (
            "output/not-a-sheet.xlsx",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ),
        ("output/not-office.txt", "application/zip"),
        ("output/legacy.doc", "application/zip"),
        ("output/not-odt.odt", "application/octet-stream"),
    ],
)
def test_office_detection_rejects_ambiguous_or_mismatched_bytes(
    monkeypatch, path: str, detected: str
) -> None:
    from orchestrator.services import canvas_files

    class OfficeMagic:
        @staticmethod
        def from_buffer(data: bytes, mime: bool = False) -> str:
            del data, mime
            return detected

    monkeypatch.setattr(canvas_files, "magic", OfficeMagic)
    with pytest.raises(CanvasFileError) as error:
        validate_canvas_bytes(path, b"PK\x03\x04mismatch")
    assert error.value.code in {"mime_renderer_mismatch", "unsupported_canvas_file"}


def test_office_renderer_is_closed_and_office_size_is_independent(monkeypatch) -> None:
    from orchestrator.services import canvas_files

    class OfficeMagic:
        @staticmethod
        def from_buffer(data: bytes, mime: bool = False) -> str:
            del data, mime
            return "application/zip"

    monkeypatch.setattr(canvas_files, "magic", OfficeMagic)
    monkeypatch.setattr(canvas_files, "MAX_TEXT_BYTES", 4)
    monkeypatch.setattr(canvas_files, "MAX_OFFICE_BYTES", 16)
    monkeypatch.setattr(canvas_files, "MAX_FILE_BYTES", 32)
    payload = b"PK\x03\x04office"

    assert (
        validate_canvas_bytes(
            "output/report.docx",
            payload,
            requested_renderer="office",
        ).renderer
        == "office"
    )
    with pytest.raises(CanvasFileError) as mismatch:
        validate_canvas_bytes(
            "output/report.docx",
            payload,
            requested_renderer="text",
        )
    assert mismatch.value.code == "mime_renderer_mismatch"

    with pytest.raises(CanvasFileError) as too_large:
        validate_canvas_bytes("output/report.docx", b"PK" + b"x" * 15)
    assert too_large.value.status_code == 413


def test_interactive_html_renderer_is_explicit_and_html_only(monkeypatch) -> None:
    from orchestrator.services import canvas_files

    monkeypatch.setattr(canvas_files, "magic", _Magic)
    data = b"<!doctype html><style>.card{color:green}</style><div class=card>Safe</div>"
    detected = validate_canvas_bytes("output/mockup.html", data)
    interactive = validate_canvas_bytes(
        "output/mockup.html",
        data,
        requested_renderer="html-interactive",
    )

    assert detected.renderer == "html"
    assert interactive.renderer == "html-interactive"
    with pytest.raises(CanvasFileError, match="incompatible"):
        validate_canvas_bytes(
            "output/mockup.md",
            b"# Mockup\n",
            requested_renderer="html-interactive",
        )


def test_javascript_mime_is_narrowly_treated_as_text(monkeypatch) -> None:
    from orchestrator.services import canvas_files

    class JavaScriptMagic:
        @staticmethod
        def from_buffer(data: bytes, mime: bool = False) -> str:
            del data, mime
            return "application/javascript"

    monkeypatch.setattr(canvas_files, "magic", JavaScriptMagic)
    validated = validate_canvas_bytes("app/main.js", b"export const safe = true;\n")
    assert validated.renderer == "text"

    class BinaryMagic:
        @staticmethod
        def from_buffer(data: bytes, mime: bool = False) -> str:
            del data, mime
            return "application/octet-stream"

    monkeypatch.setattr(canvas_files, "magic", BinaryMagic)
    with pytest.raises(CanvasFileError) as error:
        validate_canvas_bytes("app/main.js", b"export const safe = true;\n")
    assert error.value.code == "mime_renderer_mismatch"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "symlink_path",
    [
        "/home/agent-host/workspace",
        "/home/agent-host/workspace/output/report.md",
    ],
)
async def test_sftp_reader_rejects_root_and_source_symlinks(symlink_path) -> None:
    from types import SimpleNamespace

    from orchestrator.services.canvas_files import _read_sftp_workspace_file

    class FakeSFTP:
        async def lstat(self, path):
            mode = stat.S_IFLNK if path == symlink_path else stat.S_IFDIR
            if path.endswith("report.md") and path != symlink_path:
                mode = stat.S_IFREG
            return SimpleNamespace(permissions=mode | 0o700, size=9, mtime=None)

        async def realpath(self, path):
            return path

        def open(self, path, mode):
            raise AssertionError(f"unsafe path was opened: {path} {mode}")

    with pytest.raises(CanvasFileError) as error:
        await _read_sftp_workspace_file(
            FakeSFTP(),
            root="/home/agent-host/workspace",
            path="output/report.md",
        )
    assert error.value.code == "canvas_symlink_rejected"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("path", "expected_read"),
    [
        ("output/report.md", 12),
        ("output/preview.html", 12),
        ("output/report.docx", 24),
        ("output/image.png", 18),
        ("output/unknown.bin", 18),
    ],
)
async def test_sftp_transport_uses_renderer_specific_read_ceiling(
    monkeypatch, path, expected_read
) -> None:
    from types import SimpleNamespace

    from orchestrator.services import canvas_files

    monkeypatch.setattr(canvas_files, "MAX_TEXT_BYTES", 11)
    monkeypatch.setattr(canvas_files, "MAX_OFFICE_BYTES", 23)
    monkeypatch.setattr(canvas_files, "MAX_IMAGE_BYTES", 17)
    monkeypatch.setattr(canvas_files, "MAX_FILE_BYTES", 100)
    reads: list[int] = []

    class RemoteFile:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        async def read(self, limit):
            reads.append(limit)
            return b""

    class FakeSFTP:
        async def lstat(self, current):
            is_file = current.endswith(PurePosixPath(path).name)
            mode = stat.S_IFREG if is_file else stat.S_IFDIR
            return SimpleNamespace(permissions=mode | 0o700, size=None, mtime=None)

        async def realpath(self, current):
            return current

        def open(self, current, mode):
            assert current.endswith(path)
            assert mode == "rb"
            return RemoteFile()

    await canvas_files._read_sftp_workspace_file(
        FakeSFTP(), root="/home/agent-host/workspace", path=path
    )
    assert reads == [expected_read]


@pytest.mark.asyncio
async def test_sftp_rejects_known_text_size_before_open(monkeypatch) -> None:
    from types import SimpleNamespace

    from orchestrator.services import canvas_files

    monkeypatch.setattr(canvas_files, "MAX_TEXT_BYTES", 11)
    monkeypatch.setattr(canvas_files, "MAX_FILE_BYTES", 100)

    class FakeSFTP:
        async def lstat(self, current):
            is_file = current.endswith("report.md")
            mode = stat.S_IFREG if is_file else stat.S_IFDIR
            return SimpleNamespace(
                permissions=mode | 0o700,
                size=12 if is_file else None,
                mtime=None,
            )

        async def realpath(self, current):
            return current

        def open(self, current, mode):
            raise AssertionError(f"oversized file was opened: {current} {mode}")

    with pytest.raises(CanvasFileError) as error:
        await canvas_files._read_sftp_workspace_file(
            FakeSFTP(),
            root="/home/agent-host/workspace",
            path="output/report.md",
        )
    assert error.value.status_code == 413


def test_remote_endpoint_must_match_binding_and_uses_fixed_root() -> None:
    from orchestrator.services.canvas_files import (
        REMOTE_WORKSPACE_ROOT,
        _remote_workspace_target,
        _require_same_remote_workspace,
    )

    thread = _thread()
    thread["metadata"]["workspace_container"]["workspace_path"] = "/"
    host, port, root, fingerprint = _remote_workspace_target(thread, GENERATION)
    assert (host, port, root, fingerprint) == (
        "workspace.test",
        22,
        REMOTE_WORKSPACE_ROOT,
        "SHA256:test",
    )

    thread["metadata"]["workspace_container"]["_canvas_workspace_generation"] = None
    with pytest.raises(CanvasFileError) as error:
        _remote_workspace_target(thread, GENERATION)
    assert error.value.code == "workspace_generation_changed"

    replacement = _thread(generation=UUID("22222222-bbbb-4bbb-8bbb-222222222222"))
    with pytest.raises(CanvasFileError) as error:
        _require_same_remote_workspace(
            replacement,
            GENERATION,
            ("workspace.test", 22, REMOTE_WORKSPACE_ROOT, "SHA256:test"),
        )
    assert error.value.code == "workspace_generation_changed"


@pytest.mark.asyncio
async def test_pinned_sftp_options_keep_callback_validation_enabled(
    monkeypatch,
) -> None:
    from types import SimpleNamespace

    from orchestrator.services import canvas_files

    if canvas_files.asyncssh is None:
        pytest.skip("asyncssh is not installed in this unit-test environment")

    captured: dict[str, Any] = {}

    class FakeSFTP:
        def exit(self):
            return None

        async def wait_closed(self):
            return None

    class FakeConnection:
        closed = False

        async def start_sftp_client(self):
            return FakeSFTP()

        def is_closed(self):
            return self.closed

        def close(self):
            self.closed = True

        async def wait_closed(self):
            return None

    connection = FakeConnection()

    async def connect(host, **kwargs):
        captured.update(host=host, **kwargs)
        return connection

    monkeypatch.setattr(canvas_files.asyncssh, "connect", connect)
    pool = canvas_files._PinnedSFTPPool(max_entries=1)
    async with pool.checkout(
        thread_id=THREAD_ID,
        generation=GENERATION,
        host="workspace.test",
        port=22,
        fingerprint="SHA256:expected",
        key_path="/tmp/id_ed25519",
    ):
        pass

    assert captured["known_hosts"] == ((), (), (), (), (), (), ())
    assert captured["known_hosts"]
    validator = captured["client_factory"]()
    mismatched_key = SimpleNamespace(
        get_fingerprint=lambda algorithm: (
            "SHA256:other" if algorithm == "sha256" else ""
        )
    )
    assert (
        validator.validate_host_public_key(
            "workspace.test", "127.0.0.1", 22, mismatched_key
        )
        is False
    )
    await pool.evict_thread(THREAD_ID)


@pytest.mark.asyncio
async def test_sftp_start_failure_closes_new_ssh_connection(monkeypatch) -> None:
    from orchestrator.services import canvas_files

    if canvas_files.asyncssh is None:
        pytest.skip("asyncssh is not installed in this unit-test environment")

    class FakeConnection:
        closed = False
        waited = False

        async def start_sftp_client(self):
            raise RuntimeError("SFTP subsystem unavailable")

        def close(self):
            self.closed = True

        async def wait_closed(self):
            self.waited = True

    connection = FakeConnection()

    async def connect(*args, **kwargs):
        del args, kwargs
        return connection

    monkeypatch.setattr(canvas_files.asyncssh, "connect", connect)
    pool = canvas_files._PinnedSFTPPool(max_entries=1)
    with pytest.raises(RuntimeError, match="SFTP subsystem"):
        async with pool.checkout(
            thread_id=THREAD_ID,
            generation=GENERATION,
            host="workspace.test",
            port=22,
            fingerprint="SHA256:expected",
            key_path="/tmp/id_ed25519",
        ):
            pass
    assert connection.closed is True
    assert connection.waited is True


@pytest.mark.asyncio
async def test_queued_remote_read_aborts_after_release_and_reassignment(
    monkeypatch,
) -> None:
    from contextlib import asynccontextmanager

    from orchestrator.services import canvas_files

    if canvas_files.asyncssh is None:
        pytest.skip("asyncssh is not installed in this unit-test environment")

    from orchestrator.services.docker_provisioner import DockerProvisioner

    next_generation = UUID("22222222-bbbb-4bbb-8bbb-222222222222")
    initial = _thread()
    initial_context = initial["metadata"]["workspace_container"]
    initial_context.update(
        {
            "host": "workspace.test",
            "port": 30022,
            "provisioner": "docker",
            "_docker_workspace_lease_id": "lease-old",
        }
    )
    touched_sftp = False
    queued = asyncio.Event()
    holder_release = asyncio.Event()
    pool_lock = asyncio.Lock()

    class NeverReadSFTP:
        async def lstat(self, path):
            nonlocal touched_sftp
            touched_sftp = True
            raise AssertionError(f"stale reader touched reassigned host: {path}")

    class FakePool:
        @asynccontextmanager
        async def checkout(self, **kwargs):
            del kwargs
            if pool_lock.locked():
                queued.set()
            async with pool_lock:
                yield NeverReadSFTP()

        async def evict_thread(self, thread_id):
            del thread_id

    pool = FakePool()

    class DB:
        def __init__(self):
            self.thread = initial

        async def get_thread(self, thread_id):
            assert thread_id == THREAD_ID
            return self.thread

        async def transition_docker_workspace_lease(self, **kwargs):
            context = self.thread["metadata"]["workspace_container"]
            assert context["status"] in kwargs["expected_statuses"]
            context.update(kwargs["updates"])
            return dict(context)

        async def acquire_docker_workspace_lease(self, **kwargs):
            assert kwargs["owner_kind"] == "thread"
            context = {
                **kwargs["candidates"][0],
                "status": "ready",
                "provisioner": "docker",
                "_docker_workspace_lease_id": "lease-new",
                "_docker_workspace_attested": True,
                "_canvas_workspace_generation": None,
            }
            self.thread["metadata"]["workspace_container"] = context
            return dict(context)

        async def bind_thread_workspace_backing(self, *args, **kwargs):
            del args
            self.thread["metadata"]["_workspace_binding"] = {
                "generation": str(next_generation),
                "kind": "remote",
                "backing_id": kwargs["backing_id"],
                "ssh_host_key_fingerprint": kwargs["ssh_host_key_fingerprint"],
            }
            return {"workspace_generation": str(next_generation), "changed": True}

        async def merge_thread_workspace_context(self, thread_id, updates):
            assert thread_id == THREAD_ID
            self.thread["metadata"]["workspace_container"].update(updates)
            return True

        async def record_docker_workspace_process_zero(
            self, owner_id, *, owner_kind, lease_id
        ):
            assert (owner_kind, owner_id, lease_id) == (
                "thread",
                THREAD_ID,
                "lease-old",
            )
            return True

    db = DB()

    async def current_thread():
        return db.thread

    async def hold_pool_lease():
        async with pool.checkout():
            await holder_release.wait()

    monkeypatch.setattr(canvas_files, "_SFTP_POOL", pool)
    monkeypatch.setattr(canvas_files, "resolve_ssh_key_path", lambda: "/tmp/key")
    holder = asyncio.create_task(hold_pool_lease())
    for _ in range(100):
        if pool_lock.locked():
            break
        await asyncio.sleep(0)
    assert pool_lock.locked()
    read = asyncio.create_task(
        canvas_files._read_remote_default(
            initial,
            "output/report.md",
            GENERATION,
            generation_resolver=current_thread,
        )
    )
    await asyncio.wait_for(queued.wait(), timeout=1)

    monkeypatch.setenv("WORKSPACE_HOSTS", "workspace.test:30022")
    monkeypatch.setenv("DOCKER_WORKSPACE_TRUSTED_DEV_REUSE", "true")
    monkeypatch.setenv(
        "WORKSPACE_HOST_KEY_FINGERPRINTS", "workspace.test:30022=SHA256:test"
    )
    provisioner = DockerProvisioner()
    provisioner.connect(db=db)

    async def reset(host, port):
        assert (host, port) == ("workspace.test", 30022)
        return True

    monkeypatch.setattr(provisioner, "_reset_workspace_via_ssh", reset)
    assert await provisioner.release_thread_workspace(THREAD_ID) is True
    assigned = await provisioner.assign_thread_workspace(THREAD_ID)
    assert assigned is not None
    assert assigned["_canvas_workspace_generation"] == str(next_generation)

    holder_release.set()
    await holder
    with pytest.raises(CanvasFileError) as error:
        await read
    assert error.value.code == "workspace_generation_changed"
    assert touched_sftp is False


@pytest.mark.asyncio
async def test_gateway_checks_generation_and_source_version(monkeypatch) -> None:
    from orchestrator.services import canvas_files

    monkeypatch.setattr(canvas_files, "magic", _Magic)
    reads: list[str] = []

    async def loader(thread, path, generation):
        reads.append(path)
        assert generation == GENERATION
        return RawWorkspaceFile(b"# Current\n", NOW)

    thread = _thread()
    gateway = ThreadWorkspaceFileGateway(remote_loader=loader)
    generation, current = await gateway.validate_for_presentation(
        thread, "output/report.md"
    )
    assert generation == GENERATION
    record = _record(current.data)
    materialized = await gateway.materialize_current(thread, record)
    assert materialized.source_version == record.source_version
    assert reads == ["output/report.md", "output/report.md"]

    stale = replace(record, source_version="sha256:" + "0" * 64)
    with pytest.raises(CanvasFileError) as error:
        await gateway.materialize_current(thread, stale)
    assert error.value.code == "source_changed"

    rotated = _thread(generation=UUID("22222222-bbbb-4bbb-8bbb-222222222222"))
    with pytest.raises(CanvasFileError) as error:
        await gateway.materialize_current(rotated, record)
    assert error.value.code == "workspace_generation_changed"


@pytest.mark.asyncio
async def test_validation_runs_off_loop_and_has_bounded_capacity(monkeypatch) -> None:
    import threading

    from orchestrator.services import canvas_files

    main_thread = threading.get_ident()
    validation_threads: list[int] = []
    original = canvas_files.validate_canvas_bytes

    def observed_validation(*args, **kwargs):
        validation_threads.append(threading.get_ident())
        return original(*args, **kwargs)

    async def loader(thread, path, generation):
        del thread, path, generation
        return RawWorkspaceFile(b"# Safe\n", NOW)

    monkeypatch.setattr(canvas_files, "magic", _Magic)
    monkeypatch.setattr(canvas_files, "validate_canvas_bytes", observed_validation)
    gateway = ThreadWorkspaceFileGateway(remote_loader=loader)
    await gateway.validate_for_presentation(_thread(), "output/report.md")
    assert validation_threads and validation_threads[0] != main_thread

    saturated = asyncio.Semaphore(1)
    await saturated.acquire()
    monkeypatch.setattr(canvas_files, "_VALIDATION_SEMAPHORE", saturated)
    monkeypatch.setattr(canvas_files, "MATERIALIZATION_QUEUE_TIMEOUT", 0.01)
    try:
        with pytest.raises(CanvasFileError) as error:
            await gateway.validate_for_presentation(_thread(), "output/report.md")
    finally:
        saturated.release()
    assert error.value.code == "canvas_capacity_exhausted"


@pytest.mark.asyncio
async def test_full_gate_bounds_buffers_and_survives_worker_cancellation(
    monkeypatch,
) -> None:
    import threading

    from orchestrator.services import canvas_files

    worker_started = threading.Event()
    worker_exit = threading.Event()
    loader_calls = 0
    original = canvas_files.validate_canvas_bytes

    def blocked_validation(*args, **kwargs):
        worker_started.set()
        worker_exit.wait(timeout=5)
        return original(*args, **kwargs)

    async def loader(thread, path, generation):
        nonlocal loader_calls
        del thread, path, generation
        loader_calls += 1
        return RawWorkspaceFile(b"# retained while validating\n", NOW)

    monkeypatch.setattr(canvas_files, "magic", _Magic)
    monkeypatch.setattr(canvas_files, "validate_canvas_bytes", blocked_validation)
    monkeypatch.setattr(
        canvas_files, "_FULL_MATERIALIZATION_SEMAPHORE", asyncio.Semaphore(1)
    )
    monkeypatch.setattr(canvas_files, "MATERIALIZATION_QUEUE_TIMEOUT", 0.01)
    gateway = ThreadWorkspaceFileGateway(remote_loader=loader)

    first = asyncio.create_task(
        gateway.validate_for_presentation(_thread(), "output/report.md")
    )
    for _ in range(100):
        if worker_started.is_set():
            break
        await asyncio.sleep(0.005)
    assert worker_started.is_set()
    assert loader_calls == 1

    first.cancel()
    with pytest.raises(CanvasFileError) as error:
        await gateway.validate_for_presentation(_thread(), "output/report.md")
    assert error.value.code == "canvas_capacity_exhausted"
    # The cancelled request still owns the global lease until its blocking
    # validator exits, so the second request never loads/retains another file.
    assert loader_calls == 1
    assert first.done() is False

    worker_exit.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await gateway.validate_for_presentation(_thread(), "output/report.md")
    assert loader_calls == 2


@pytest.mark.asyncio
async def test_virtual_materialization_saturation_fails_boundedly(monkeypatch) -> None:
    from orchestrator.services import canvas_files
    from orchestrator.services.workspace_binding import virtual_thread_backing_id

    spec = {
        "type": "s3",
        "root": "bucket/canvas",
        "config": {"endpoint": "https://objects.test"},
    }
    thread = _thread(backend="virtual")
    thread["metadata"]["_workspace_binding"]["backing_id"] = virtual_thread_backing_id(
        THREAD_ID, spec
    )
    saturated = asyncio.Semaphore(1)
    await saturated.acquire()
    monkeypatch.setattr(canvas_files, "_VIRTUAL_SEMAPHORE", saturated)
    monkeypatch.setattr(canvas_files, "MATERIALIZATION_QUEUE_TIMEOUT", 0.01)
    monkeypatch.setattr(canvas_files, "virtual_workspace_rclone_spec", lambda: spec)
    monkeypatch.setattr(canvas_files.shutil, "which", lambda name: f"/usr/bin/{name}")

    try:
        with pytest.raises(CanvasFileError) as error:
            await canvas_files._read_virtual_default(
                thread, "output/report.md", GENERATION
            )
    finally:
        saturated.release()
    assert error.value.code == "canvas_capacity_exhausted"


@pytest.mark.asyncio
async def test_virtual_rclone_growth_is_bounded_and_children_are_reaped(
    monkeypatch, tmp_path
) -> None:
    from orchestrator.services import canvas_files
    from orchestrator.services.workspace_binding import virtual_thread_backing_id
    from shared.runtime.core.backends.rclone import (
        RcloneObjectStore,
        RcloneSizeLimitExceeded,
    )

    _write_streaming_rclone(tmp_path)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")
    limit = 1024

    direct_marker = tmp_path / "direct-terminated"
    direct_started = tmp_path / "direct-started"
    store = RcloneObjectStore(
        remote_type="s3",
        config={
            "mode": "overflow",
            "marker": str(direct_marker),
            "started": str(direct_started),
        },
        root="bucket",
        transfer_timeout=2,
    )
    with pytest.raises(RcloneSizeLimitExceeded) as overflow:
        store.get_bounded("threads/id/report.md", limit)
    assert overflow.value.observed == limit + 1
    direct_pid = int(direct_marker.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(direct_pid, 0)

    canvas_marker = tmp_path / "canvas-terminated"
    canvas_started = tmp_path / "canvas-started"
    spec = {
        "type": "s3",
        "root": "bucket/canvas",
        "config": {
            "mode": "overflow",
            "marker": str(canvas_marker),
            "started": str(canvas_started),
        },
    }
    thread = _thread(backend="virtual")
    thread["metadata"]["_workspace_binding"]["backing_id"] = virtual_thread_backing_id(
        THREAD_ID, spec
    )
    monkeypatch.setattr(canvas_files, "MAX_FILE_BYTES", limit)
    monkeypatch.setattr(canvas_files, "virtual_workspace_rclone_spec", lambda: spec)

    with pytest.raises(CanvasFileError) as error:
        await canvas_files._read_virtual_default(thread, "output/report.md", GENERATION)
    assert error.value.status_code == 413
    canvas_pid = int(canvas_marker.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(canvas_pid, 0)


@pytest.mark.asyncio
async def test_virtual_rclone_cancellation_terminates_and_reaps_child(
    monkeypatch, tmp_path
) -> None:
    from orchestrator.services import canvas_files
    from orchestrator.services.workspace_binding import virtual_thread_backing_id

    _write_streaming_rclone(tmp_path)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")
    marker = tmp_path / "cancel-terminated"
    started = tmp_path / "cancel-started"
    spec = {
        "type": "s3",
        "root": "bucket/canvas",
        "config": {
            "mode": "hang",
            "marker": str(marker),
            "started": str(started),
        },
    }
    thread = _thread(backend="virtual")
    thread["metadata"]["_workspace_binding"]["backing_id"] = virtual_thread_backing_id(
        THREAD_ID, spec
    )
    monkeypatch.setattr(canvas_files, "virtual_workspace_rclone_spec", lambda: spec)

    reading = asyncio.create_task(
        canvas_files._read_virtual_default(thread, "output/report.md", GENERATION)
    )
    for _ in range(200):
        if started.exists():
            break
        await asyncio.sleep(0.01)
    assert started.exists()
    reading.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(reading, timeout=2)
    child_pid = int(marker.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(child_pid, 0)


@pytest.mark.asyncio
async def test_remote_materialization_queue_is_bounded(monkeypatch) -> None:
    from orchestrator.services import canvas_files

    saturated = asyncio.Semaphore(1)
    await saturated.acquire()
    monkeypatch.setattr(canvas_files, "_REMOTE_SEMAPHORE", saturated)
    monkeypatch.setattr(canvas_files, "MATERIALIZATION_QUEUE_TIMEOUT", 0.01)
    try:
        with pytest.raises(CanvasFileError) as error:
            async with canvas_files._materialization_slot(
                canvas_files._REMOTE_SEMAPHORE
            ):
                raise AssertionError("saturated slot must not be entered")
    finally:
        saturated.release()
    assert error.value.code == "canvas_capacity_exhausted"


class _RouteDB:
    def __init__(self, thread: dict) -> None:
        self.thread = thread
        self.owner_allowed = True
        self.owner_checks = 0

    async def get_thread(self, thread_id: str) -> dict | None:
        return self.thread if thread_id == THREAD_ID else None


class _RouteGateway:
    def __init__(self, file: ValidatedCanvasFile) -> None:
        self.file = file
        self.materialize_calls = 0
        self.error: CanvasFileError | None = None
        self.after_materialize = None
        self.after_validate = None
        self.editing_supported = True
        self.edit_candidate_calls = 0

    async def materialize_current(self, thread, record):
        del thread, record
        self.materialize_calls += 1
        if self.error:
            raise self.error
        if self.after_materialize:
            self.after_materialize()
        return self.file

    async def validate_for_presentation(
        self, thread, path, *, requested_renderer="auto", alt_text=None
    ):
        del thread, path, requested_renderer, alt_text
        if self.after_validate:
            self.after_validate()
        return GENERATION, self.file

    def supports_editing(self, thread, record=None):
        del thread, record
        return self.editing_supported

    async def validate_edit_candidate(self, record, data):
        del record
        self.edit_candidate_calls += 1
        return replace(self.file, data=data, source_version=_file(data).source_version)

    async def replace_current(self, thread, record, data, *, expected_source_version):
        del thread, record
        if self.file.source_version != expected_source_version:
            raise CanvasFileError(
                412,
                "canvas_content_precondition_failed",
                "stale",
            )
        self.file = _file(data)
        return self.file

    async def materialize_for_refresh(self, thread, record):
        del thread, record
        return self.file


class _RouteService:
    def __init__(self, record: CanvasRecord | None) -> None:
        self.record = record

    async def get(self, thread_id: str):
        assert thread_id == THREAD_ID
        return self.record

    async def clear(
        self,
        thread_id: str,
        *,
        expected_etag: str | None,
        representation_builder=build_public_canvas_representation,
        require_precondition: bool = True,
        **kwargs,
    ):
        del kwargs
        assert thread_id == THREAD_ID
        if self.record is None:
            return CanvasMutation(changed=False, record=None)
        if self.record.source is None:
            return CanvasMutation(changed=False, record=self.record)
        if (
            require_precondition
            and representation_builder(self.record).etag != expected_etag
        ):
            raise CanvasPreconditionFailed("stale")
        self.record = replace(
            self.record,
            source=None,
            title=None,
            renderer="auto",
            alt_text=None,
            presentation_revision=self.record.presentation_revision + 1,
            source_fingerprint=None,
            source_version=None,
            updated_at=NOW,
        )
        return CanvasMutation(changed=True, record=self.record)

    async def set(self, thread_id: str, presentation):
        assert thread_id == THREAD_ID
        source = presentation.source
        revision = (self.record.presentation_revision if self.record else 0) + 1
        self.record = CanvasRecord(
            thread_id=THREAD_ID,
            canvas_id="main",
            source=source,
            title=presentation.title,
            renderer=presentation.renderer,
            editable=presentation.editable,
            alt_text=presentation.alt_text,
            presentation_revision=revision,
            source_fingerprint=canonical_source_fingerprint(source),
            source_version=presentation.source_version,
            origin_generation=None,
            created_at=NOW,
            updated_at=NOW,
        )
        return CanvasMutation(changed=True, record=self.record)

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
        assert thread_id == THREAD_ID
        assert expected_thread_user_id == USER_ID
        assert self.record is not None
        if self.record.source_fingerprint != expected_source_fingerprint:
            raise CanvasFileError(409, "canvas_replaced", "replaced")
        if self.record.presentation_revision != expected_presentation_revision:
            raise CanvasFileError(409, "canvas_presentation_changed", "changed")
        if self.record.source_version != expected_source_version:
            raise CanvasFileError(412, "canvas_content_precondition_failed", "stale")
        version = await writer(self.record, _thread())
        self.record = replace(
            self.record,
            source_version=version,
            presentation_revision=self.record.presentation_revision + 1,
        )
        return CanvasMutation(changed=True, record=self.record)

    async def refresh_file(
        self,
        thread_id,
        *,
        expected_etag,
        expected_thread_user_id,
        refresher,
        representation_builder,
        **kwargs,
    ):
        del kwargs
        assert thread_id == THREAD_ID and self.record is not None
        assert expected_thread_user_id == USER_ID
        if representation_builder(self.record).etag != expected_etag:
            raise CanvasPreconditionFailed("stale")
        version = await refresher(self.record, _thread())
        self.record = replace(
            self.record,
            source_version=version,
            presentation_revision=self.record.presentation_revision + 1,
        )
        return CanvasMutation(changed=True, record=self.record)


def _route_client(
    monkeypatch,
    *,
    record: CanvasRecord | None = None,
    file: ValidatedCanvasFile | None = None,
):
    from orchestrator.routers import canvases

    db = _RouteDB(_thread())
    service = _RouteService(record)
    gateway = _RouteGateway(file or _file())

    async def owner(request, received_db, thread_id):
        assert received_db is db and thread_id == THREAD_ID
        db.owner_checks += 1
        if not db.owner_allowed:
            from fastapi import HTTPException

            raise HTTPException(status_code=403, detail="revoked")
        return {"id": USER_ID}, db.thread

    async def internal(request):
        assert request.headers.get("X-Internal-Key") == "test-key"

    monkeypatch.setattr(canvases, "_get_canvas_service", lambda received: service)
    monkeypatch.setattr(canvases, "_get_file_gateway", lambda received=None: gateway)
    monkeypatch.setattr(canvases, "require_thread_owner", owner)
    monkeypatch.setattr(canvases, "require_internal", internal)
    app = FastAPI()
    app.state.store = db
    app.include_router(canvases.router)
    app.include_router(canvases.internal_router)
    return TestClient(app), service, gateway, db


def test_state_content_head_range_and_conditional_clear(monkeypatch) -> None:
    client, service, _, _ = _route_client(monkeypatch, record=_record())
    state_url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"
    state = client.get(state_url)
    assert state.status_code == 200
    assert state.json()["status"] == "ready"
    assert state.json()["capabilities"]["can_pop_out"] is True
    content_url = state.json()["content_url"]
    assert content_url and parse_qs(urlsplit(content_url).query)["ngsw-bypass"] == [
        "true"
    ]

    content = client.get(content_url)
    assert content.status_code == 200
    assert content.content == b"# Canvas\n"
    assert content.headers["etag"] == f'"{service.record.source_version}"'
    assert content.headers["x-content-type-options"] == "nosniff"

    head = client.head(content_url)
    assert head.status_code == 200 and not head.content
    assert head.headers["content-length"] == str(len(b"# Canvas\n"))

    partial = client.get(content_url, headers={"Range": "bytes=2-7"})
    assert partial.status_code == 206
    assert partial.content == b"Canvas"
    assert partial.headers["content-range"] == "bytes 2-7/9"

    ignored = client.get(
        content_url,
        headers={"Range": "bytes=2-7", "If-Range": '"sha256:stale"'},
    )
    assert ignored.status_code == 200 and ignored.content == b"# Canvas\n"

    invalid = client.get(content_url, headers={"Range": "bytes=+1-2"})
    assert invalid.status_code == 416
    assert invalid.headers["content-range"] == "bytes */9"
    assert invalid.headers["content-length"] == "0"

    cached = client.get(content_url, headers={"If-None-Match": content.headers["etag"]})
    assert cached.status_code == 304 and not cached.content
    assert "content-length" not in cached.headers

    cleared = client.delete(state_url, headers={"If-Match": state.headers["etag"]})
    assert cleared.status_code == 200
    assert cleared.json()["status"] == "cleared"


def test_stale_content_identities_are_typed_and_ngsw_is_required(monkeypatch) -> None:
    client, service, _, _ = _route_client(monkeypatch, record=_record())
    state = client.get(f"/api/persistent/threads/{THREAD_ID}/canvases/main")
    content_url = state.json()["content_url"]
    split = urlsplit(content_url)
    query = parse_qs(split.query)
    query["source_fingerprint"] = ["sha256:" + "f" * 64]
    from urllib.parse import urlencode

    stale = client.get(split.path + "?" + urlencode(query, doseq=True))
    assert stale.status_code == 409
    assert stale.json()["detail"]["code"] == "canvas_replaced"

    no_bypass = {key: values for key, values in query.items() if key != "ngsw-bypass"}
    missing = client.get(split.path + "?" + urlencode(no_bypass, doseq=True))
    assert missing.status_code == 422

    service.record = replace(service.record, source=None)
    cleared = client.get(content_url)
    assert cleared.status_code == 409
    assert cleared.json()["detail"]["code"] == "canvas_cleared"


def test_content_rechecks_owner_after_materialization(monkeypatch) -> None:
    client, _, gateway, db = _route_client(monkeypatch, record=_record())
    state = client.get(f"/api/persistent/threads/{THREAD_ID}/canvases/main")
    assert state.status_code == 200
    checks_before_content = db.owner_checks
    gateway.after_materialize = lambda: setattr(db, "owner_allowed", False)

    response = client.get(state.json()["content_url"])
    assert response.status_code == 403
    assert db.owner_checks == checks_before_content + 2


def test_public_state_rechecks_owner_after_remote_representation(monkeypatch) -> None:
    client, _, gateway, db = _route_client(monkeypatch, record=_record())
    gateway.after_materialize = lambda: setattr(db, "owner_allowed", False)

    response = client.get(f"/api/persistent/threads/{THREAD_ID}/canvases/main")

    assert response.status_code == 403
    assert db.owner_checks == 2


def test_delegated_state_rechecks_owner_after_remote_representation(
    monkeypatch,
) -> None:
    client, _, gateway, db = _route_client(monkeypatch, record=_record())
    gateway.after_materialize = lambda: setattr(db, "owner_allowed", False)

    response = client.get(
        f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main",
        headers={"X-Internal-Key": "test-key", "X-MCP-User-Id": USER_ID},
    )

    assert response.status_code == 403
    assert db.owner_checks == 2


@pytest.mark.asyncio
async def test_response_capacity_is_held_through_final_asgi_body_send(
    monkeypatch,
) -> None:
    from orchestrator.routers import canvases
    from orchestrator.services import canvas_files

    semaphore = asyncio.Semaphore(1)
    await semaphore.acquire()
    lease = canvas_files.CanvasResponseLease(semaphore)
    monkeypatch.setattr(canvas_files, "_RESPONSE_BUFFER_SEMAPHORE", semaphore)
    monkeypatch.setattr(canvas_files, "MATERIALIZATION_QUEUE_TIMEOUT", 0.01)
    body_started = asyncio.Event()
    allow_body = asyncio.Event()

    async def send(message):
        if message["type"] == "http.response.body":
            body_started.set()
            await allow_body.wait()

    async def receive():
        return {"type": "http.disconnect"}

    response = canvases._LeasedContentResponse(content=b"retained", lease=lease)
    sending = asyncio.create_task(
        response(
            {"type": "http", "method": "GET", "path": "/canvas"},
            receive,
            send,
        )
    )
    await asyncio.wait_for(body_started.wait(), timeout=1)

    with pytest.raises(CanvasFileError) as error:
        await canvas_files.acquire_canvas_response_lease()
    assert error.value.code == "canvas_capacity_exhausted"

    allow_body.set()
    await sending
    next_lease = await canvas_files.acquire_canvas_response_lease()
    next_lease.release()


@pytest.mark.asyncio
async def test_response_cancellation_releases_buffer_lease_exactly_once() -> None:
    from orchestrator.routers import canvases
    from orchestrator.services.canvas_files import CanvasResponseLease

    class CountingSemaphore:
        releases = 0

        def release(self):
            self.releases += 1

    semaphore = CountingSemaphore()
    lease = CanvasResponseLease(semaphore)  # type: ignore[arg-type]
    body_started = asyncio.Event()
    never = asyncio.Event()

    async def send(message):
        if message["type"] == "http.response.body":
            body_started.set()
            await never.wait()

    async def receive():
        return {"type": "http.disconnect"}

    response = canvases._LeasedContentResponse(content=b"retained", lease=lease)
    sending = asyncio.create_task(
        response(
            {"type": "http", "method": "GET", "path": "/canvas"},
            receive,
            send,
        )
    )
    await asyncio.wait_for(body_started.wait(), timeout=1)
    sending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sending
    assert semaphore.releases == 1
    lease.release()
    assert semaphore.releases == 1


def test_internal_file_set_contract_and_serializer_boundary(monkeypatch) -> None:
    client, service, _, _ = _route_client(monkeypatch)
    url = f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main/set"
    headers = {"X-Internal-Key": "test-key", "X-MCP-User-Id": USER_ID}

    missing_kind = client.post(url, headers=headers, json={"path": "output/report.md"})
    assert missing_kind.status_code == 422
    mixed = client.post(
        url,
        headers=headers,
        json={
            "source_type": "workspace_file",
            "path": "output/report.md",
            "port": 8501,
        },
    )
    assert mixed.status_code == 422

    accepted = client.post(
        url,
        headers=headers,
        json={"source_type": "workspace_file", "path": "output/report.md"},
    )
    assert accepted.status_code == 200
    assert accepted.json()["content_url"] is None
    assert accepted.json()["editable"] is False
    assert service.record is not None and service.record.presentation_revision == 1

    mismatch = client.get(
        f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main",
        headers={"X-Internal-Key": "test-key", "X-MCP-User-Id": "wrong"},
    )
    assert mismatch.status_code == 401


def test_internal_office_set_is_enabled_only_with_live_collabora(
    monkeypatch,
) -> None:
    from orchestrator.routers import canvases
    from orchestrator.services.canvas_office import CollaboraConfig

    office = _office_file()
    client, service, gateway, _ = _route_client(monkeypatch, file=office)
    url = f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main/set"
    headers = {"X-Internal-Key": "test-key", "X-MCP-User-Id": USER_ID}
    base_config = dict(
        internal_url="http://collabora:9980",
        public_origin="https://office.example.test",
        wopi_base_url="http://orchestrator:8085",
        cockpit_origin="https://cockpit.example.test",
        token_ttl_seconds=36_000,
        discovery_cache_ttl_seconds=60,
        request_timeout_seconds=2.0,
    )

    monkeypatch.setattr(
        canvases,
        "_get_collabora_config",
        lambda: CollaboraConfig(enabled=False, **base_config),
    )
    disabled = client.post(
        url,
        headers=headers,
        json={"source_type": "workspace_file", "path": office.path},
    )
    assert disabled.status_code == 422
    assert disabled.json()["detail"]["code"] == "canvas_office_unavailable"
    assert service.record is None

    class Discovery:
        async def get_urlsrc(self, extension):
            assert extension == ".docx"
            return "https://office.example.test/browser/hash/cool.html?"

    monkeypatch.setattr(
        canvases,
        "_get_collabora_config",
        lambda: CollaboraConfig(enabled=True, **base_config),
    )
    monkeypatch.setattr(canvases, "_get_office_discovery", Discovery)
    gateway.editing_supported = False
    unwritable = client.post(
        url,
        headers=headers,
        json={
            "source_type": "workspace_file",
            "path": office.path,
            "editable": True,
        },
    )
    assert unwritable.status_code == 422
    assert unwritable.json()["detail"]["code"] == "canvas_editing_unsupported"
    assert service.record is None

    gateway.editing_supported = True
    editable = client.post(
        url,
        headers=headers,
        json={
            "source_type": "workspace_file",
            "path": office.path,
            "editable": True,
        },
    )
    assert editable.status_code == 200
    assert editable.json()["renderer"] == "office"
    assert editable.json()["editable"] is True
    assert editable.json()["capabilities"]["can_edit"] is True
    assert editable.json()["capabilities"]["can_view_office"] is True
    assert service.record is not None

    accepted = client.post(
        url,
        headers=headers,
        json={"source_type": "workspace_file", "path": office.path},
    )
    assert accepted.status_code == 200
    assert accepted.json()["renderer"] == "office"
    assert accepted.json()["content_url"] is None
    assert accepted.json()["capabilities"]["can_view_office"] is True
    assert accepted.json()["editable"] is False


def test_office_bytes_never_use_the_generic_canvas_content_route(monkeypatch) -> None:
    office = _office_file()
    source = WorkspaceFileSource(path=office.path, workspace_generation=GENERATION)
    record = CanvasRecord(
        thread_id=THREAD_ID,
        canvas_id="main",
        source=source,
        title="Office report",
        renderer="office",
        editable=False,
        alt_text=None,
        presentation_revision=3,
        source_fingerprint=canonical_source_fingerprint(source),
        source_version=office.source_version,
        origin_generation=None,
        created_at=NOW,
        updated_at=NOW,
    )
    client, _, gateway, _ = _route_client(
        monkeypatch,
        record=record,
        file=office,
    )
    response = client.get(
        f"/api/persistent/threads/{THREAD_ID}/canvases/main/content",
        params={
            "presentation_revision": record.presentation_revision,
            "source_fingerprint": record.source_fingerprint,
            "source_version": record.source_version,
            "ngsw-bypass": "true",
        },
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "canvas_office_content_unavailable"
    assert gateway.materialize_calls == 0

    put = client.put(
        f"/api/persistent/threads/{THREAD_ID}/canvases/main/content",
        params={
            "presentation_revision": record.presentation_revision,
            "source_fingerprint": record.source_fingerprint,
            "source_version": record.source_version,
            "ngsw-bypass": "true",
        },
        headers={
            "If-Match": f'"{record.source_version}"',
            "X-Canvas-Presentation-Revision": str(record.presentation_revision),
            "Content-Type": "application/octet-stream",
        },
        content=office.data,
    )
    assert put.status_code == 409
    assert put.json()["detail"]["code"] == "canvas_office_content_unavailable"
    assert gateway.edit_candidate_calls == 0


def test_office_session_mint_requires_bff_cookie_and_returns_form_contract(
    monkeypatch,
) -> None:
    from orchestrator.routers import canvases
    from orchestrator.services.canvas_office import CollaboraConfig, WopiTokenGrant

    office = _office_file()
    source = WorkspaceFileSource(path=office.path, workspace_generation=GENERATION)
    record = CanvasRecord(
        thread_id=THREAD_ID,
        canvas_id="main",
        source=source,
        title="Office report",
        renderer="office",
        editable=False,
        alt_text=None,
        presentation_revision=3,
        source_fingerprint=canonical_source_fingerprint(source),
        source_version=office.source_version,
        origin_generation=None,
        created_at=NOW,
        updated_at=NOW,
    )
    client, _, _, _ = _route_client(monkeypatch, record=record, file=office)
    config = CollaboraConfig(
        enabled=True,
        internal_url="http://collabora:9980",
        public_origin="https://office.example.test",
        wopi_base_url="http://orchestrator:8085",
        cockpit_origin="https://cockpit.example.test",
        token_ttl_seconds=36_000,
        discovery_cache_ttl_seconds=60,
        request_timeout_seconds=2.0,
    )

    class Discovery:
        async def get_urlsrc(self, extension):
            assert extension == ".docx"
            return "https://office.example.test/browser/hash/cool.html?"

    class Tokens:
        def mint(self, **kwargs):
            assert kwargs == {
                "user_id": USER_ID,
                "thread_id": THREAD_ID,
                "path": office.path,
                "write_flag": False,
            }
            return WopiTokenGrant(
                access_token="signed-token",
                file_id="f" * 64,
                expires_at_ms=1_721_858_400_000,
            )

    monkeypatch.setattr(canvases, "_get_collabora_config", lambda: config)
    monkeypatch.setattr(canvases, "_get_office_discovery", Discovery)
    monkeypatch.setattr(canvases, "_get_wopi_token_service", lambda db: Tokens())

    state_url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"
    state = client.get(state_url)
    assert state.status_code == 200
    session_url = f"{state_url}/office-session"
    missing_cookie = client.post(
        session_url,
        headers={
            "If-Match": state.headers["etag"],
            "Origin": config.cockpit_origin,
        },
        content=b"",
    )
    assert missing_cookie.status_code == 401

    session = client.post(
        session_url,
        headers={
            "If-Match": state.headers["etag"],
            "Origin": config.cockpit_origin,
        },
        cookies={"srw_session": "33333333-3333-4333-8333-333333333333"},
        content=b"",
    )
    assert session.status_code == 200
    assert session.headers["cache-control"] == "private, no-store"
    assert session.json() == {
        "urlsrc": "https://office.example.test/browser/hash/cool.html?",
        "WOPISrc": f"http://orchestrator:8085/wopi/files/{'f' * 64}",
        "access_token": "signed-token",
        "access_token_ttl": 1_721_858_400_000,
    }


def test_office_session_accepts_the_weakened_form_of_its_own_state_etag(
    monkeypatch,
) -> None:
    """The Office pane holds whatever ETag the CDN handed the browser."""

    from orchestrator.routers import canvases
    from orchestrator.services.canvas_office import CollaboraConfig, WopiTokenGrant

    office = _office_file()
    source = WorkspaceFileSource(path=office.path, workspace_generation=GENERATION)
    record = CanvasRecord(
        thread_id=THREAD_ID,
        canvas_id="main",
        source=source,
        title="Office report",
        renderer="office",
        editable=False,
        alt_text=None,
        presentation_revision=3,
        source_fingerprint=canonical_source_fingerprint(source),
        source_version=office.source_version,
        origin_generation=None,
        created_at=NOW,
        updated_at=NOW,
    )
    client, _, _, _ = _route_client(monkeypatch, record=record, file=office)
    config = CollaboraConfig(
        enabled=True,
        internal_url="http://collabora:9980",
        public_origin="https://office.example.test",
        wopi_base_url="http://orchestrator:8085",
        cockpit_origin="https://cockpit.example.test",
        token_ttl_seconds=36_000,
        discovery_cache_ttl_seconds=60,
        request_timeout_seconds=2.0,
    )

    class Discovery:
        async def get_urlsrc(self, extension):
            return "https://office.example.test/browser/hash/cool.html?"

    class Tokens:
        def mint(self, **kwargs):
            return WopiTokenGrant(
                access_token="signed-token",
                file_id="f" * 64,
                expires_at_ms=1_721_858_400_000,
            )

    monkeypatch.setattr(canvases, "_get_collabora_config", lambda: config)
    monkeypatch.setattr(canvases, "_get_office_discovery", Discovery)
    monkeypatch.setattr(canvases, "_get_wopi_token_service", lambda db: Tokens())

    state_url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"
    state = client.get(state_url)
    assert state.status_code == 200

    session = client.post(
        f"{state_url}/office-session",
        headers={
            "If-Match": f"W/{state.headers['etag']}",
            "Origin": config.cockpit_origin,
        },
        cookies={"srw_session": "33333333-3333-4333-8333-333333333333"},
        content=b"",
    )

    assert session.status_code == 200
    assert session.json()["access_token"] == "signed-token"

    other_state = client.post(
        f"{state_url}/office-session",
        headers={
            "If-Match": 'W/"canvas:3:' + "0" * 64 + '"',
            "Origin": config.cockpit_origin,
        },
        cookies={"srw_session": "33333333-3333-4333-8333-333333333333"},
        content=b"",
    )

    assert other_state.status_code == 412
    assert other_state.json()["detail"]["code"] == "canvas_precondition_failed"


def test_editable_office_session_mints_write_scope_and_capability(monkeypatch) -> None:
    from orchestrator.routers import canvases
    from orchestrator.services.canvas_office import CollaboraConfig, WopiTokenGrant

    office = _office_file()
    source = WorkspaceFileSource(path=office.path, workspace_generation=GENERATION)
    record = CanvasRecord(
        thread_id=THREAD_ID,
        canvas_id="main",
        source=source,
        title="Office report",
        renderer="office",
        editable=True,
        alt_text=None,
        presentation_revision=3,
        source_fingerprint=canonical_source_fingerprint(source),
        source_version=office.source_version,
        origin_generation=None,
        created_at=NOW,
        updated_at=NOW,
    )
    client, _, _, _ = _route_client(monkeypatch, record=record, file=office)
    config = CollaboraConfig(
        enabled=True,
        internal_url="http://collabora:9980",
        public_origin="https://office.example.test",
        wopi_base_url="http://orchestrator:8085",
        cockpit_origin="https://cockpit.example.test",
        token_ttl_seconds=36_000,
        discovery_cache_ttl_seconds=60,
        request_timeout_seconds=2.0,
    )
    minted: list[dict[str, Any]] = []

    class Discovery:
        async def get_urlsrc(self, extension):
            assert extension == ".docx"
            return "https://office.example.test/browser/hash/cool.html?"

    class Tokens:
        def mint(self, **kwargs):
            minted.append(kwargs)
            return WopiTokenGrant(
                access_token="write-token",
                file_id="e" * 64,
                expires_at_ms=1_721_858_400_000,
            )

    monkeypatch.setattr(canvases, "_get_collabora_config", lambda: config)
    monkeypatch.setattr(canvases, "_get_office_discovery", Discovery)
    monkeypatch.setattr(canvases, "_get_wopi_token_service", lambda db: Tokens())

    state_url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"
    state = client.get(state_url)
    assert state.status_code == 200
    assert state.json()["capabilities"]["can_edit"] is True
    session = client.post(
        f"{state_url}/office-session",
        headers={
            "If-Match": state.headers["etag"],
            "Origin": config.cockpit_origin,
        },
        cookies={"srw_session": "33333333-3333-4333-8333-333333333333"},
        content=b"",
    )
    assert session.status_code == 200
    assert minted == [
        {
            "user_id": USER_ID,
            "thread_id": THREAD_ID,
            "path": office.path,
            "write_flag": True,
        }
    ]


def test_internal_set_rechecks_delegated_owner_before_commit(monkeypatch) -> None:
    client, service, gateway, db = _route_client(monkeypatch)
    gateway.after_validate = lambda: setattr(db, "owner_allowed", False)
    response = client.post(
        f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main/set",
        headers={"X-Internal-Key": "test-key", "X-MCP-User-Id": USER_ID},
        json={"source_type": "workspace_file", "path": "output/report.md"},
    )
    assert response.status_code == 403
    assert service.record is None
    assert db.owner_checks == 2


def test_internal_set_rejects_generation_rotation_after_validation(monkeypatch) -> None:
    client, service, gateway, db = _route_client(monkeypatch)

    def rotate_workspace() -> None:
        replacement = _thread(generation=UUID("22222222-bbbb-4bbb-8bbb-222222222222"))
        db.thread = replacement

    gateway.after_validate = rotate_workspace
    response = client.post(
        f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main/set",
        headers={"X-Internal-Key": "test-key", "X-MCP-User-Id": USER_ID},
        json={"source_type": "workspace_file", "path": "output/report.md"},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "workspace_generation_changed"
    assert service.record is None
    assert db.owner_checks == 2


def test_internal_repeated_clear_returns_existing_state_without_revision_bump(
    monkeypatch,
) -> None:
    already_cleared = replace(
        _record(),
        source=None,
        title=None,
        renderer="auto",
        source_fingerprint=None,
        source_version=None,
        presentation_revision=7,
    )
    client, service, _, _ = _route_client(monkeypatch, record=already_cleared)
    url = f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main"
    headers = {"X-Internal-Key": "test-key", "X-MCP-User-Id": USER_ID}

    first = client.delete(url, headers=headers)
    second = client.delete(url, headers=headers)

    assert first.status_code == second.status_code == 200
    assert first.json()["status"] == second.json()["status"] == "cleared"
    assert first.json()["presentation_revision"] == 7
    assert second.json()["presentation_revision"] == 7
    assert first.headers["x-canvas-mutation-changed"] == "false"
    assert second.headers["x-canvas-mutation-changed"] == "false"
    assert service.record is not None
    assert service.record.presentation_revision == 7


def test_internal_clear_returns_204_only_when_canvas_was_never_created(
    monkeypatch,
) -> None:
    client, _, _, _ = _route_client(monkeypatch, record=None)
    response = client.delete(
        f"/api/internal/persistent/threads/{THREAD_ID}/canvases/main",
        headers={"X-Internal-Key": "test-key", "X-MCP-User-Id": USER_ID},
    )
    assert response.status_code == 204


def test_editable_content_put_uses_both_preconditions_and_returns_new_state(
    monkeypatch,
) -> None:
    client, service, gateway, _ = _route_client(
        monkeypatch, record=_record(editable=True)
    )
    state = client.get(f"/api/persistent/threads/{THREAD_ID}/canvases/main")
    content_url = state.json()["content_url"]
    content_etag = f'"{service.record.source_version}"'

    saved = client.put(
        content_url,
        content=b"# User edit\n",
        headers={
            "If-Match": content_etag,
            "X-Canvas-Presentation-Revision": str(service.record.presentation_revision),
            "Content-Type": "text/markdown; charset=utf-8",
        },
    )

    assert saved.status_code == 200
    assert saved.json()["editable"] is True
    assert saved.json()["capabilities"]["can_edit"] is True
    assert saved.json()["capabilities"]["can_pop_out"] is True
    assert saved.json()["presentation_revision"] == 2
    assert saved.json()["source_version"] == gateway.file.source_version
    assert saved.headers["x-canvas-content-etag"] == (
        f'"{gateway.file.source_version}"'
    )
    assert saved.headers["etag"].startswith('"canvas:2:')
    assert gateway.file.data == b"# User edit\n"


def test_content_put_rejects_missing_malformed_and_stale_preconditions_before_write(
    monkeypatch,
) -> None:
    client, service, gateway, _ = _route_client(
        monkeypatch, record=_record(editable=True)
    )
    state = client.get(f"/api/persistent/threads/{THREAD_ID}/canvases/main")
    content_url = state.json()["content_url"]
    original = gateway.file.data

    missing = client.put(content_url, content=b"# rejected\n")
    assert missing.status_code == 428
    assert missing.json()["detail"]["code"] == "canvas_precondition_required"

    malformed = client.put(
        content_url,
        content=b"# rejected\n",
        headers={
            "If-Match": "*",
            "X-Canvas-Presentation-Revision": "1",
        },
    )
    assert malformed.status_code == 400
    assert malformed.json()["detail"]["code"] == "invalid_canvas_precondition"

    stale = client.put(
        content_url,
        content=b"# rejected\n",
        headers={
            "If-Match": '"sha256:' + "0" * 64 + '"',
            "X-Canvas-Presentation-Revision": "1",
        },
    )
    assert stale.status_code == 412
    assert stale.json()["detail"]["code"] == ("canvas_content_precondition_failed")
    assert gateway.file.data == original
    assert service.record.presentation_revision == 1


def test_content_put_reports_same_source_republish_before_hash_conflict(
    monkeypatch,
) -> None:
    client, service, gateway, _ = _route_client(
        monkeypatch, record=_record(editable=True)
    )
    state = client.get(f"/api/persistent/threads/{THREAD_ID}/canvases/main")
    old_url = state.json()["content_url"]
    service.record = replace(service.record, presentation_revision=2)

    response = client.put(
        old_url,
        content=b"# rejected\n",
        headers={
            "If-Match": '"sha256:' + "0" * 64 + '"',
            "X-Canvas-Presentation-Revision": "1",
        },
    )

    assert response.status_code == 409
    assert response.json()["detail"]["code"] == "canvas_presentation_changed"
    assert gateway.file.data == b"# Canvas\n"


def test_refresh_adopts_drifted_file_with_the_visible_state_etag(monkeypatch) -> None:
    client, service, gateway, _ = _route_client(
        monkeypatch, record=_record(editable=True)
    )
    gateway.file = _file(b"# Agent changed this outside Canvas\n")
    url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"
    drifted = client.get(url)
    assert drifted.status_code == 200
    assert drifted.json()["status"] == "source_changed"
    assert drifted.json()["content_url"] is None

    refreshed = client.post(
        f"{url}/refresh",
        headers={"If-Match": drifted.headers["etag"]},
    )

    assert refreshed.status_code == 200
    assert refreshed.json()["status"] == "ready"
    assert refreshed.json()["presentation_revision"] == 2
    assert refreshed.json()["source_version"] == gateway.file.source_version
    assert refreshed.headers["x-canvas-content-etag"] == (
        f'"{gateway.file.source_version}"'
    )
    assert service.record.source_version == gateway.file.source_version


def test_refresh_accepts_the_weakened_form_of_its_own_state_etag(monkeypatch) -> None:
    """A CDN-weakened ETag names the same state; the shape gate must admit it."""

    client, service, gateway, _ = _route_client(
        monkeypatch, record=_record(editable=True)
    )
    gateway.file = _file(b"# Agent changed this outside Canvas\n")
    url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"
    drifted = client.get(url)
    assert drifted.status_code == 200

    refreshed = client.post(
        f"{url}/refresh",
        headers={"If-Match": f"W/{drifted.headers['etag']}"},
    )

    assert refreshed.status_code == 200
    assert refreshed.json()["presentation_revision"] == 2
    assert service.record.source_version == gateway.file.source_version


def test_refresh_refuses_a_weakened_etag_for_another_state(monkeypatch) -> None:
    """Admitting the weak marker must not admit a different state."""

    client, service, gateway, _ = _route_client(
        monkeypatch, record=_record(editable=True)
    )
    gateway.file = _file(b"# Agent changed this outside Canvas\n")
    url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"

    response = client.post(
        f"{url}/refresh",
        headers={"If-Match": 'W/"canvas:1:' + "0" * 64 + '"'},
    )

    assert response.status_code == 412
    assert response.json()["detail"]["code"] == "canvas_precondition_failed"
    assert service.record.presentation_revision == 1


def test_refresh_rejects_a_caller_supplied_source_body(monkeypatch) -> None:
    client, service, _, _ = _route_client(monkeypatch, record=_record(editable=True))
    url = f"/api/persistent/threads/{THREAD_ID}/canvases/main"
    state = client.get(url)

    response = client.post(
        f"{url}/refresh",
        headers={"If-Match": state.headers["etag"]},
        json={"path": "other.md"},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "invalid_canvas_refresh"
    assert service.record.presentation_revision == 1


class _BindingConnection:
    def __init__(self) -> None:
        self.metadata: dict[str, Any] = {}

    @asynccontextmanager
    async def transaction(self):
        yield

    async def fetchrow(self, query: str, *args):
        del query, args
        return {"metadata": self.metadata}

    async def execute(self, query: str, *args):
        if "_workspace_binding" in str(args[1]):
            self.metadata.update(json.loads(args[1]))
        elif "{workspace_container}" in query:
            self.metadata.setdefault("workspace_container", {}).update(
                json.loads(args[0])
            )
        return "UPDATE 1"


class _DockerLeaseConnection:
    """In-memory asyncpg seam with a DB-global advisory transaction lock."""

    def __init__(self) -> None:
        self.jobs: dict[str, dict[str, Any]] = {}
        self.threads: dict[str, dict[str, Any]] = {}
        self.inventory: dict[tuple[str, int], dict[str, Any]] = {}
        self.process_zero_receipts: set[tuple[str, str, str]] = set()
        self.lock = asyncio.Lock()
        self.advisory_calls = 0

    @asynccontextmanager
    async def transaction(self):
        async with self.lock:
            yield

    async def fetchrow(self, query: str, *args):
        if "FROM docker_workspace_leases" in query:
            if "WHERE owner_kind = $1 AND owner_id = $2 AND lease_id = $3" in query:
                owner_kind, owner_id, lease_id = args
                row = next(
                    (
                        item
                        for item in self.inventory.values()
                        if item.get("owner_kind") == owner_kind
                        and str(item.get("owner_id")) == str(owner_id)
                        and str(item.get("lease_id")) == str(lease_id)
                    ),
                    None,
                )
            elif "WHERE owner_kind = $1 AND owner_id = $2" in query:
                owner_kind, owner_id = args
                row = next(
                    (
                        item
                        for item in self.inventory.values()
                        if item.get("owner_kind") == owner_kind
                        and str(item.get("owner_id")) == str(owner_id)
                        and item.get("status") in {"ready", "releasing"}
                    ),
                    None,
                )
            elif "owner_kind = $3 AND owner_id = $4" in query:
                host, port, owner_kind, owner_id = args
                candidate = self.inventory.get((str(host), int(port)))
                row = (
                    candidate
                    if candidate
                    and candidate.get("owner_kind") == owner_kind
                    and str(candidate.get("owner_id")) == str(owner_id)
                    and candidate.get("lease_id") is None
                    else None
                )
            else:
                host, port = args
                row = self.inventory.get((str(host), int(port)))
            return dict(row) if row is not None else None

        owner_id = args[0]
        table = self.jobs if "FROM jobs" in query else self.threads
        key = str(owner_id)
        if key not in table:
            return None
        return {"workspace": table[key]}

    async def fetchval(self, query: str, *args):
        assert "managed_repository_process_zero_receipts" in query
        owner_kind, owner_id, lease_id = args
        return (str(owner_kind), str(owner_id), str(lease_id)) in (
            self.process_zero_receipts
        )

    async def fetch(self, query: str):
        assert "UNION ALL" in query
        rows = []
        for workspace in (*self.jobs.values(), *self.threads.values()):
            if (
                workspace.get("provisioner") == "docker"
                and workspace.get("status") != "released"
            ):
                rows.append({"workspace": dict(workspace)})
        return rows

    async def execute(self, query: str, *args):
        if "pg_advisory_xact_lock" in query:
            self.advisory_calls += 1
            return "SELECT 1"
        if "INSERT INTO docker_workspace_leases" in query:
            host, port = str(args[0]), int(args[1])
            if (host, port) not in self.inventory:
                if len(args) == 6:
                    status, trust_mode, fingerprint, reason = args[2:]
                else:
                    status = "quarantined"
                    trust_mode = "unattested"
                    fingerprint = None
                    reason = "owner_inventory_missing"
                self.inventory[(host, port)] = {
                    "host": host,
                    "port": port,
                    "status": status,
                    "lease_id": None,
                    "owner_kind": None,
                    "owner_id": None,
                    "trust_mode": trust_mode,
                    "host_key_fingerprint": fingerprint,
                    "quarantine_reason": reason,
                }
            return "INSERT 0 1"
        if "UPDATE docker_workspace_leases" in query:
            host, port = str(args[0]), int(args[1])
            row = self.inventory[(host, port)]
            if "SET status = 'ready'" in query:
                row.update(
                    {
                        "status": "ready",
                        "lease_id": args[2],
                        "owner_kind": args[3],
                        "owner_id": args[4],
                        "quarantine_reason": None,
                    }
                )
            elif "SET status = $4" in query:
                row.update(
                    {
                        "status": args[3],
                        "trust_mode": args[4],
                        "host_key_fingerprint": args[5],
                        "quarantine_reason": args[6],
                    }
                )
            else:
                row["status"] = "quarantined"
                if "owner_mirror_mismatch" in query:
                    row["quarantine_reason"] = "owner_mirror_mismatch"
                elif "owner_deleted_during_transition" in query:
                    row["quarantine_reason"] = "owner_deleted_during_transition"
                elif "host_fingerprint_inventory_mismatch" in query:
                    row["quarantine_reason"] = "host_fingerprint_inventory_mismatch"
                elif "trusted_dev_inventory_not_enabled" in query:
                    row["quarantine_reason"] = "trusted_dev_inventory_not_enabled"
                elif "inventory_trust_invalid" in query:
                    row["quarantine_reason"] = "inventory_trust_invalid"
            return "UPDATE 1"
        if query.lstrip().startswith("UPDATE threads AS target"):
            current = self.threads.setdefault(str(args[0]), {})
            current.update({"repo_name": args[1], "git_remote_url": args[2]})
            return "UPDATE 1"
        if "COALESCE(metadata->'workspace_container'" in query:
            current = self.threads.setdefault(str(args[1]), {})
            current.update(json.loads(args[0]))
            return "UPDATE 1"
        if "'{workspace_container}', $2::jsonb" in query:
            table = self.jobs if query.startswith("UPDATE jobs") else self.threads
            table[str(args[0])] = json.loads(args[1])
            return "UPDATE 1"
        table = self.jobs if query.startswith("UPDATE jobs") else self.threads
        table[str(args[0])] = json.loads(args[1])
        return "UPDATE 1"


def _postgres_over(connection: _DockerLeaseConnection) -> PostgresDB:
    db = PostgresDB.__new__(PostgresDB)

    @asynccontextmanager
    async def acquire():
        yield connection

    db.acquire = acquire
    return db


@pytest.mark.asyncio
async def test_atomic_docker_pool_is_shared_by_jobs_threads_and_replicas() -> None:
    job_id = "c5555555-5555-4555-8555-555555555555"
    thread_id = "d6666666-6666-4666-8666-666666666666"
    connection = _DockerLeaseConnection()
    connection.jobs[job_id] = {}
    connection.threads[thread_id] = {}
    first_replica = _postgres_over(connection)
    second_replica = _postgres_over(connection)
    candidates = [
        {
            "host": "workspace-1",
            "port": 30022,
            "trusted_dev_reuse": True,
        }
    ]

    job_lease, thread_lease = await asyncio.gather(
        first_replica.acquire_docker_workspace_lease(
            owner_kind="job", owner_id=job_id, candidates=candidates
        ),
        second_replica.acquire_docker_workspace_lease(
            owner_kind="thread", owner_id=thread_id, candidates=candidates
        ),
    )
    assert (job_lease is None) != (thread_lease is None)
    assert connection.advisory_calls == 2

    winner_kind, winner_id, winner = (
        ("job", job_id, job_lease)
        if job_lease is not None
        else ("thread", thread_id, thread_lease)
    )
    loser_kind, loser_id = (
        ("thread", thread_id) if winner_kind == "job" else ("job", job_id)
    )
    assert winner is not None
    quarantined = await first_replica.transition_docker_workspace_lease(
        owner_kind=winner_kind,
        owner_id=winner_id,
        expected_lease_id=winner["_docker_workspace_lease_id"],
        expected_statuses={"ready"},
        updates={"status": "quarantined"},
    )
    assert quarantined and quarantined["status"] == "quarantined"
    assert (
        await second_replica.acquire_docker_workspace_lease(
            owner_kind=loser_kind, owner_id=loser_id, candidates=candidates
        )
        is None
    )


@pytest.mark.asyncio
async def test_legacy_lease_id_none_is_cas_not_wildcard_after_reassignment() -> None:
    job_id = "e7777777-7777-4777-8777-777777777777"
    connection = _DockerLeaseConnection()
    connection.jobs[job_id] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "provisioner": "docker",
    }
    connection.inventory[("workspace-1", 30022)] = {
        "host": "workspace-1",
        "port": 30022,
        "status": "ready",
        "lease_id": None,
        "owner_kind": "job",
        "owner_id": UUID(job_id),
        "trust_mode": "trusted_dev",
        "host_key_fingerprint": None,
        "quarantine_reason": None,
    }
    db = _postgres_over(connection)

    claimed = await db.transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=job_id,
        expected_lease_id=None,
        expected_statuses={"ready"},
        updates={"status": "releasing"},
    )
    assert claimed is not None
    old_lease_id = claimed["_docker_workspace_lease_id"]
    connection.process_zero_receipts.add(("job", job_id, str(old_lease_id)))
    released = await db.transition_docker_workspace_lease(
        owner_kind="job",
        owner_id=job_id,
        expected_lease_id=old_lease_id,
        expected_statuses={"releasing"},
        updates={
            "status": "released",
            "_docker_workspace_trust_mode": "trusted_dev",
            "_docker_workspace_attested": False,
        },
    )
    assert released is not None
    reassigned = await db.acquire_docker_workspace_lease(
        owner_kind="job",
        owner_id=job_id,
        candidates=[
            {
                "host": "workspace-1",
                "port": 30022,
                "trusted_dev_reuse": True,
            }
        ],
    )
    assert reassigned is not None
    assert reassigned["_docker_workspace_lease_id"] != old_lease_id

    assert (
        await db.transition_docker_workspace_lease(
            owner_kind="job",
            owner_id=job_id,
            expected_lease_id=None,
            expected_statuses={"ready"},
            updates={"status": "releasing"},
        )
        is None
    )
    assert connection.jobs[job_id]["status"] == "ready"


@pytest.mark.asyncio
async def test_docker_allocator_preserves_prior_repository_and_snapshot_metadata() -> (
    None
):
    thread_id = "d6666666-6666-4666-8666-666666666667"
    connection = _DockerLeaseConnection()
    connection.threads[thread_id] = {
        "git_remote_url": "http://gitea/thread.git",
        "repo_name": "thread-d6666666",
        "last_snapshot": "snapshots/thread-d6666666.tar.zst",
        "last_snapshot_turns": 9,
        # Every lifecycle/identity value below must be replaced or dropped.
        "host": "old-workspace",
        "port": 22,
        "status": "released",
        "provisioner": "docker",
        "_docker_workspace_lease_id": "old-lease",
        "_canvas_workspace_generation": str(GENERATION),
        "quarantine_reason": "old-failure",
        "snapshot_attempts": 4,
    }
    db = _postgres_over(connection)

    lease = await db.acquire_docker_workspace_lease(
        owner_kind="thread",
        owner_id=thread_id,
        candidates=[
            {
                "host": "workspace-1",
                "port": 30022,
                "trusted_dev_reuse": True,
            }
        ],
    )

    assert lease is not None
    assert lease["git_remote_url"] == "http://gitea/thread.git"
    assert lease["repo_name"] == "thread-d6666666"
    assert lease["last_snapshot"] == "snapshots/thread-d6666666.tar.zst"
    assert lease["last_snapshot_turns"] == 9
    assert lease["host"] == "workspace-1"
    assert lease["port"] == 30022
    assert lease["_docker_workspace_lease_id"] != "old-lease"
    assert lease["_canvas_workspace_generation"] is None
    assert lease["quarantine_reason"] is None
    assert "snapshot_attempts" not in lease


@pytest.mark.asyncio
async def test_repository_merge_after_docker_allocation_preserves_lease_identity() -> (
    None
):
    thread_id = "d6666666-6666-4666-8666-666666666668"
    connection = _DockerLeaseConnection()
    connection.threads[thread_id] = {}
    db = _postgres_over(connection)

    lease = await db.acquire_docker_workspace_lease(
        owner_kind="thread",
        owner_id=thread_id,
        candidates=[
            {
                "host": "workspace-1",
                "port": 30022,
                "trusted_dev_reuse": True,
            }
        ],
    )
    assert lease is not None
    lease_id = lease["_docker_workspace_lease_id"]

    assert await db.bind_thread_managed_repository(
        thread_id,
        clean_url="http://gitea/thread.git",
        repo_name="thread-d6666666",
    )
    merged = connection.threads[thread_id]
    assert merged["git_remote_url"] == "http://gitea/thread.git"
    assert merged["repo_name"] == "thread-d6666666"
    assert merged["host"] == "workspace-1"
    assert merged["status"] == "ready"
    assert merged["_docker_workspace_lease_id"] == lease_id
    assert merged["_canvas_workspace_generation"] is None


@pytest.mark.asyncio
async def test_workspace_binding_is_idempotent_and_status_merges_do_not_rotate() -> (
    None
):
    connection = _BindingConnection()
    db = PostgresDB.__new__(PostgresDB)

    @asynccontextmanager
    async def acquire():
        yield connection

    db.acquire = acquire
    first = await db.bind_thread_workspace_backing(
        THREAD_ID,
        backing_kind="remote",
        backing_id="pod:one",
        ssh_host_key_fingerprint="SHA256:first",
    )
    assert first and first["changed"]
    generation = first["workspace_generation"]

    await db.merge_thread_workspace_context(THREAD_ID, {"status": "suspended"})
    repeated = await db.bind_thread_workspace_backing(
        THREAD_ID,
        backing_kind="remote",
        backing_id="pod:one",
        ssh_host_key_fingerprint="SHA256:first",
    )
    assert repeated == {"workspace_generation": generation, "changed": False}

    replacement = await db.bind_thread_workspace_backing(
        THREAD_ID,
        backing_kind="remote",
        backing_id="pod:two",
        ssh_host_key_fingerprint="SHA256:second",
    )
    assert replacement and replacement["workspace_generation"] != generation


def test_private_workspace_binding_is_removed_from_public_thread_shapes() -> None:
    from orchestrator.services.thread_projection import redact_thread_metadata

    private = _thread()["metadata"]["_workspace_binding"]
    for metadata in (
        {
            "_workspace_binding": private,
            "vm": {
                "status": "ready",
                "_canvas_workspace_generation": str(GENERATION),
                "_docker_workspace_lease_id": "private-lease",
                "_docker_workspace_trust_mode": "trusted_dev",
                "_docker_workspace_attested": True,
                "_docker_workspace_host_key_fingerprint": "SHA256:private",
                "quarantine_reason": "private-reason",
            },
        },
        json.dumps(
            {
                "_workspace_binding": private,
                "workspace_container": {
                    "_canvas_workspace_generation": str(GENERATION),
                    "_docker_workspace_lease_id": "private-lease",
                    "_docker_workspace_trust_mode": "trusted_dev",
                    "_docker_workspace_attested": True,
                    "_docker_workspace_host_key_fingerprint": "SHA256:private",
                    "quarantine_reason": "private-reason",
                },
            }
        ),
    ):
        redacted = redact_thread_metadata({"id": THREAD_ID, "metadata": metadata})
        serialized = json.dumps(redacted)
        assert "_workspace_binding" not in serialized
        assert "SHA256:test" not in serialized
        assert "test-backing" not in serialized
        assert str(GENERATION) not in serialized
        assert "private-lease" not in serialized
        assert "private-reason" not in serialized
        assert "trusted_dev" not in serialized
        assert "SHA256:private" not in serialized


def test_private_workspace_lease_is_removed_from_public_job_shapes() -> None:
    import orchestrator.main

    context = json.dumps(
        {
            # Top-level repo identity is the public half of the job context and
            # survives redaction; the provisioner branch below does not.
            "git_remote_url": "http://gitea/job.git",
            "workspace_container": {
                "status": "quarantined",
                "_canvas_workspace_generation": str(GENERATION),
                "_docker_workspace_lease_id": "private-job-lease",
                "_docker_workspace_trust_mode": "trusted_dev",
                "_docker_workspace_attested": True,
                "_docker_workspace_host_key_fingerprint": "SHA256:private-job",
                "quarantine_reason": "private-job-reason",
                "git_remote_url": "http://gitea/private-job-branch.git",
            },
        }
    )
    redacted = orchestrator.main._redact_job_config_override(
        {"id": "job", "context": context, "config_override": None}
    )
    assert isinstance(redacted["context"], str)
    serialized = json.dumps(redacted)
    assert str(GENERATION) not in serialized
    assert "private-job-lease" not in serialized
    assert "private-job-reason" not in serialized
    assert "trusted_dev" not in serialized
    assert "SHA256:private-job" not in serialized
    # The whole provisioner branch is dropped from the public job shape — the
    # coordinate-free workspace_contract projection is what replaces it — so
    # even its non-secret members (a nested git remote) do not leak out.
    assert "workspace_container" not in serialized
    assert "private-job-branch" not in serialized
    assert json.loads(redacted["context"]) == {"git_remote_url": "http://gitea/job.git"}
    assert redacted["workspace_contract"]["assigned_backend"] is not None


def test_workspace_host_key_material_is_root_only_and_pod_persistent() -> None:
    entrypoint = Path("docker/workspace-entrypoint.sh").read_text()
    dockerfile = Path("docker/Dockerfile.workspace").read_text()
    manifest_source = Path(
        "src/orchestrator/services/container_provisioner.py"
    ).read_text()
    assert "HOST_KEY_DIR=/var/lib/srw-system/ssh" in entrypoint
    assert "chown -R agent-host:agent-host /home/agent-host" not in entrypoint
    assert "chown -R" not in entrypoint
    assert 'install -d -o root -g root -m 0700 "$HOST_KEY_DIR"' in entrypoint
    assert (
        'chown root:root "$HOST_KEY_DIR"/ssh_host_*_key '
        '"$HOST_KEY_DIR"/ssh_host_*_key.pub' in entrypoint
    )
    assert 'chmod 0600 "$HOST_KEY_DIR"/ssh_host_*_key' in entrypoint
    assert 'chmod 0644 "$HOST_KEY_DIR"/ssh_host_*_key.pub' in entrypoint
    # Deleting the agent-owned marker can only re-run the non-recursive home
    # seed. The root identity setup always runs afterwards and reasserts modes.
    assert entrypoint.index(".workspace-initialized") < entrypoint.index(
        "HOST_KEY_DIR=/var/lib/srw-system/ssh"
    )
    assert "ssh-keygen -A" not in dockerfile
    assert '"mountPath": "/home/agent-host"' in manifest_source
    assert '"name": "workspace-identity"' in manifest_source
    assert '"mountPath": "/var/lib/srw-system"' in manifest_source


def test_remote_canvas_capability_requires_exact_trusted_generation_pair() -> None:
    from orchestrator.services.workspace_binding import (
        remote_canvas_presentation_available,
    )

    thread = _thread()
    metadata = thread["metadata"]
    workspace = metadata["workspace_container"]
    assert remote_canvas_presentation_available(metadata, workspace) is True

    for changed in (
        {**workspace, "status": "creating"},
        {**workspace, "_canvas_workspace_generation": None},
        {
            **workspace,
            "_canvas_workspace_generation": "22222222-bbbb-4bbb-8bbb-222222222222",
        },
    ):
        assert remote_canvas_presentation_available(metadata, changed) is False

    untrusted = {
        **metadata,
        "_workspace_binding": {
            **metadata["_workspace_binding"],
            "ssh_host_key_fingerprint": None,
        },
    }
    assert remote_canvas_presentation_available(untrusted, workspace) is False


@pytest.mark.asyncio
async def test_k8s_trusted_identity_tracks_pvc_not_replacement_pod(monkeypatch) -> None:
    from types import SimpleNamespace

    from orchestrator.services import container_provisioner as module

    pod_uid = ["11111111-1111-4111-8111-111111111111"]
    pvc_uid = ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"]

    class CoreAPI:
        connect_get_namespaced_pod_exec = object()

        def read_namespaced_pod(self, **kwargs):
            del kwargs
            return SimpleNamespace(metadata=SimpleNamespace(uid=pod_uid[0]))

        def read_namespaced_persistent_volume_claim(self, **kwargs):
            del kwargs
            return SimpleNamespace(metadata=SimpleNamespace(uid=pvc_uid[0]))

    def trusted_stream(*args, **kwargs):
        del args, kwargs
        return "256 SHA256:provisioner-trusted host (ED25519)"

    monkeypatch.setattr(module, "k8s_stream", trusted_stream)
    provisioner = module.ContainerProvisioner.__new__(module.ContainerProvisioner)
    provisioner._core_api = CoreAPI()
    provisioner._namespace = "agent-workspaces"

    pod_backing, fingerprint, pod_runtime = await provisioner._trusted_pod_ssh_identity(
        "workspace-pod"
    )
    assert (
        pod_backing == "k8s-pod:agent-workspaces:11111111-1111-4111-8111-111111111111"
    )
    assert fingerprint == "SHA256:provisioner-trusted"
    assert pod_runtime == pod_uid[0]

    first_claim, _, first_runtime = await provisioner._trusted_pod_ssh_identity(
        "workspace-pod", pvc_name="workspace-claim"
    )
    pod_uid[0] = "22222222-2222-4222-8222-222222222222"
    (
        replacement_pod_backing,
        _,
        replacement_pod_runtime,
    ) = await provisioner._trusted_pod_ssh_identity("replacement-pod")
    same_claim, _, replacement_runtime = await provisioner._trusted_pod_ssh_identity(
        "replacement-pod", pvc_name="workspace-claim"
    )
    # emptyDir identity follows the Pod and therefore rotates both backing and
    # runtime; PVC identity remains stable while only the runtime UID rotates.
    assert replacement_pod_backing != pod_backing
    assert replacement_pod_runtime == pod_uid[0]
    assert same_claim == first_claim
    assert replacement_runtime != first_runtime
    assert replacement_runtime == pod_uid[0]

    pvc_uid[0] = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
    replacement_claim, _, same_runtime = await provisioner._trusted_pod_ssh_identity(
        "replacement-pod", pvc_name="workspace-claim"
    )
    assert replacement_claim != first_claim
    assert same_runtime == replacement_runtime


@pytest.mark.asyncio
async def test_k8s_ready_context_pairs_backing_generation_with_pod_uid(
    monkeypatch,
) -> None:
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from orchestrator.services import container_provisioner as module

    runtime_generation = "33333333-3333-4333-8333-333333333333"
    runtime_incarnation = "22222222-2222-4222-8222-222222222222"
    backing_id = f"k8s-pod:agent-workspaces:{runtime_incarnation}"

    class _PinnedSessionDB:
        def __init__(self):
            self.merge_thread_workspace_context = AsyncMock(return_value=True)
            self.published: dict[str, str] = {}
            self.ready: dict[str, str] | None = None

        @asynccontextmanager
        async def thread_advisory_lock(self, _thread_id):
            yield True

        async def stateless_thread_workspace_creation_requires_authority(
            self, _thread_id
        ):
            return False

        async def get_workspace_network_tier(self, *_args):
            return "internet-only"

        async def get_thread(self, thread_id):
            return {
                "id": thread_id,
                "status": "active",
                "execution_lane": "pinned",
                "runtime_generation": runtime_generation,
                "runtime_retirement_token": None,
                "agent_id": None,
                "runtime_attach_token": None,
                "metadata": {"workspace_container": {"status": "deleted"}},
            }

        async def reserve_pinned_thread_workspace_provision_intent(
            self, thread_id, **kwargs
        ):
            return {
                "attempt_id": kwargs["attempt_id"],
                "thread_id": thread_id,
                "runtime_generation": kwargs["expected_runtime_generation"],
                "pod_name": kwargs["pod_name"],
                "pvc_name": kwargs["pvc_name"],
                "seed_configmap_name": kwargs["seed_configmap_name"],
                "service_name": kwargs["service_name"],
                "retained_pvc_uid": None,
                "retained_service_uid": None,
            }

        async def publish_pinned_thread_workspace_provision_resource(
            self, _thread_id, **kwargs
        ):
            self.published[kwargs["resource"]] = kwargs["resource_uid"]
            return True

        async def complete_pinned_thread_workspace_provision_intent(
            self, _thread_id, **kwargs
        ):
            if kwargs["expected_pod_uid"] != self.published.get("pod"):
                return None
            self.ready = {
                "_canvas_workspace_generation": str(GENERATION),
                module.WORKSPACE_RUNTIME_INCARNATION_KEY: kwargs["expected_pod_uid"],
            }
            return {
                "runtime_incarnation": kwargs["expected_pod_uid"],
                "workspace_generation": str(GENERATION),
                "backing_id": backing_id,
            }

    provisioner = module.ContainerProvisioner()
    provisioner._k8s_available = True
    provisioner._namespace = "agent-workspaces"
    provisioner._db = _PinnedSessionDB()
    provisioner._core_api = MagicMock()
    provisioner._pvc_enabled = False
    monkeypatch.setattr(
        provisioner, "_resolve_ide_seed_files", AsyncMock(return_value={})
    )
    monkeypatch.setattr(
        provisioner, "_resolve_ide_extensions", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        provisioner, "_resolve_ide_needs_state", AsyncMock(return_value=False)
    )

    def create_pod(**kwargs):
        body = kwargs["body"]
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=body["metadata"]["name"],
                namespace=body["metadata"]["namespace"],
                uid=runtime_incarnation,
                labels=dict(body["metadata"]["labels"]),
                annotations=dict(body["metadata"].get("annotations") or {}),
                deletion_timestamp=None,
            ),
            spec=SimpleNamespace(
                volumes=list(body["spec"]["volumes"]),
                containers=list(body["spec"]["containers"]),
                init_containers=[],
                ephemeral_containers=[],
            ),
            status=SimpleNamespace(
                phase="Running",
                pod_ip="10.42.0.8",
                container_statuses=[SimpleNamespace(ready=True)],
            ),
        )

    provisioner._core_api.create_namespaced_pod.side_effect = create_pod
    monkeypatch.setattr(
        provisioner, "_wait_for_ready", AsyncMock(return_value="10.42.0.8")
    )
    monkeypatch.setattr(
        provisioner,
        "_trusted_pod_ssh_identity",
        AsyncMock(
            return_value=(
                backing_id,
                "SHA256:" + ("A" * 43),
                runtime_incarnation,
            )
        ),
    )

    assert await provisioner.create_pinned_thread_workspace(THREAD_ID) is True

    # Exact pinned provisioning publishes Pending and Ready through the
    # intent transaction; the provisioner must not revive the legacy split
    # merge path around those writes.
    provisioner._db.merge_thread_workspace_context.assert_not_awaited()
    assert provisioner._db.ready == {
        "_canvas_workspace_generation": str(GENERATION),
        module.WORKSPACE_RUNTIME_INCARNATION_KEY: runtime_incarnation,
    }


def test_vm_host_key_ownership_modes_leave_canvas_fail_closed() -> None:
    cleanup = Path("docker/agent-vm-base/scripts/cleanup.sh").read_text()
    primary_template = Path("helm/templates/vm-controller/configmap.yaml").read_text()
    nats_source = Path("src/orchestrator/services/nats_bridge.py").read_text()
    assert "rm -f /etc/ssh/ssh_host_*" in cleanup
    # Same-cluster host identity is injected into the per-VM Secret by the
    # controller — the template must never regenerate one guest-side.
    assert "rm -f /etc/ssh/ssh_host_*" not in primary_template
    assert "ssh-keygen -A" not in primary_template
    assert "bind_thread_workspace_backing(" not in nats_source


def test_docker_fingerprint_inventory_parser(monkeypatch) -> None:
    from orchestrator.services.docker_provisioner import DockerProvisioner

    monkeypatch.setenv(
        "WORKSPACE_HOST_KEY_FINGERPRINTS",
        "workspace-1:22=SHA256:abc,broken,workspace-2:2202=SHA256:def",
    )
    assert DockerProvisioner._parse_host_fingerprints(
        "WORKSPACE_HOST_KEY_FINGERPRINTS"
    ) == {
        "workspace-1:22": "SHA256:abc",
        "workspace-2:2202": "SHA256:def",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fingerprint_inventory", "inventory_attested"),
    [
        ("", True),
        ("workspace-1:30022=SHA256:configured", False),
    ],
)
async def test_unattested_docker_thread_still_assigns_but_disables_canvas(
    monkeypatch, fingerprint_inventory, inventory_attested
) -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    monkeypatch.setenv("WORKSPACE_HOSTS", "workspace-1")
    monkeypatch.setenv("WORKSPACE_HOST_KEY_FINGERPRINTS", fingerprint_inventory)
    provisioner = DockerProvisioner()
    db = AsyncMock()
    db.acquire_docker_workspace_lease.return_value = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": "lease-unpinned",
        "_docker_workspace_attested": inventory_attested,
        # Existing/idempotent lease from before fingerprint inventory removal.
        "_canvas_workspace_generation": str(GENERATION),
    }
    db.transition_docker_workspace_lease.return_value = {
        **db.acquire_docker_workspace_lease.return_value,
        "_canvas_workspace_generation": None,
    }
    provisioner.connect(db=db)

    assigned = await provisioner.assign_thread_workspace(THREAD_ID)

    assert assigned is not None
    assert assigned["status"] == "ready"
    assert assigned["_canvas_workspace_generation"] is None
    db.bind_thread_workspace_backing.assert_not_awaited()
    db.transition_docker_workspace_lease.assert_awaited_once_with(
        owner_kind="thread",
        owner_id=THREAD_ID,
        expected_lease_id="lease-unpinned",
        expected_statuses={"ready"},
        updates={
            "status": "ready",
            "_canvas_workspace_generation": None,
        },
    )


@pytest.mark.asyncio
async def test_default_docker_job_release_retires_agents_then_quarantines() -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    db = AsyncMock()
    lease = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": "lease-job",
    }
    db.get_job.return_value = {"context": {"workspace_container": lease}}
    transitions: list[dict[str, Any]] = []

    async def transition(**kwargs):
        transitions.append(kwargs["updates"])
        return {**lease, **kwargs["updates"]}

    db.transition_docker_workspace_lease.side_effect = transition
    provisioner._db = db
    provisioner._reset_workspace_via_ssh = AsyncMock(return_value=True)
    provisioner._retire_managed_repository_agents_via_ssh = AsyncMock(return_value=True)

    assert await provisioner.release_workspace("job") is True
    provisioner._reset_workspace_via_ssh.assert_not_awaited()
    provisioner._retire_managed_repository_agents_via_ssh.assert_awaited_once_with(
        "workspace-1", 30022
    )
    db.record_docker_workspace_process_zero.assert_awaited_once_with(
        "job", owner_kind="job", lease_id="lease-job"
    )
    assert transitions == [
        {
            "status": "releasing",
            "quarantine_reason": "managed_repository_agent_retirement_claimed",
        },
        {
            "status": "quarantined",
            "quarantine_reason": "container_recreation_required_process_zero",
        },
    ]


@pytest.mark.asyncio
async def test_default_docker_thread_release_retires_then_quarantines() -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    db = AsyncMock()
    lease = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": "lease-thread",
        "_canvas_workspace_generation": str(GENERATION),
    }
    db.get_thread.return_value = {"metadata": {"workspace_container": lease}}
    transitions: list[dict[str, Any]] = []

    async def transition(**kwargs):
        transitions.append(kwargs["updates"])
        return {**lease, **kwargs["updates"]}

    db.transition_docker_workspace_lease.side_effect = transition
    provisioner._db = db
    provisioner._reset_workspace_via_ssh = AsyncMock(return_value=True)
    provisioner._retire_managed_repository_agents_via_ssh = AsyncMock(return_value=True)

    # Exact quarantine is a successful terminal release: the host is fenced
    # from allocation until controller-attested container recreation.
    assert await provisioner.release_thread_workspace(THREAD_ID) is True
    provisioner._reset_workspace_via_ssh.assert_not_awaited()
    provisioner._retire_managed_repository_agents_via_ssh.assert_awaited_once_with(
        "workspace-1", 30022
    )
    db.record_docker_workspace_process_zero.assert_awaited_once_with(
        THREAD_ID, owner_kind="thread", lease_id="lease-thread"
    )
    assert transitions == [
        {
            "status": "releasing",
            "quarantine_reason": "managed_repository_agent_retirement_claimed",
            "_canvas_workspace_generation": None,
        },
        {
            "status": "quarantined",
            "quarantine_reason": "container_recreation_required_process_zero",
            "_canvas_workspace_generation": None,
        },
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_default_docker_release_fails_closed_when_process_zero_is_unknown(
    owner_kind: str,
) -> None:
    """Quarantine is not evidence that a live static process namespace is clean."""

    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    lease = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": f"lease-{owner_kind}",
    }
    db = AsyncMock()
    if owner_kind == "job":
        db.get_job.return_value = {"context": {"workspace_container": lease}}
    else:
        db.get_thread.return_value = {"metadata": {"workspace_container": lease}}
    transitions: list[dict[str, Any]] = []

    async def transition(**kwargs):
        transitions.append(kwargs)
        return {**lease, **kwargs["updates"]}

    db.transition_docker_workspace_lease.side_effect = transition
    provisioner._db = db
    provisioner._retire_managed_repository_agents_via_ssh = AsyncMock(
        return_value=False
    )

    released = (
        await provisioner.release_workspace("job")
        if owner_kind == "job"
        else await provisioner.release_thread_workspace(THREAD_ID)
    )
    assert released is False
    assert transitions[-1]["updates"]["status"] == "quarantined"
    assert (
        transitions[-1]["updates"]["quarantine_reason"]
        == "managed_repository_agent_retirement_failed"
    )


@pytest.mark.asyncio
async def test_default_docker_release_reclaims_exact_failed_quarantine_once() -> None:
    """A supported retry can settle an old/failed exact lease without a race."""

    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    lease = {
        "status": "quarantined",
        "quarantine_reason": "managed_repository_agent_retirement_failed",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": "lease-job",
    }
    db = AsyncMock()
    db.get_job.return_value = {"context": {"workspace_container": lease}}
    transitions: list[dict[str, Any]] = []

    async def transition(**kwargs):
        transitions.append(kwargs)
        return {**lease, **kwargs["updates"]}

    db.transition_docker_workspace_lease.side_effect = transition
    provisioner._db = db
    provisioner._retire_managed_repository_agents_via_ssh = AsyncMock(return_value=True)

    assert await provisioner.release_workspace("job") is True
    assert transitions[0]["expected_statuses"] == {"quarantined"}
    assert transitions[0]["updates"] == {
        "status": "releasing",
        "quarantine_reason": "managed_repository_agent_retirement_claimed",
    }
    assert transitions[-1]["updates"] == {
        "status": "quarantined",
        "quarantine_reason": "container_recreation_required_process_zero",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
async def test_default_docker_release_replays_proven_process_zero(
    owner_kind: str,
) -> None:
    """A lost response or later teardown failure does not wedge retry."""

    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    lease = {
        "status": "quarantined",
        "quarantine_reason": "container_recreation_required_process_zero",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": f"lease-{owner_kind}",
    }
    db = AsyncMock()
    db.get_job.return_value = {"context": {"workspace_container": lease}}
    db.get_thread.return_value = {"metadata": {"workspace_container": lease}}
    db.docker_workspace_process_zero_is_current.return_value = True
    db.transition_docker_workspace_lease.return_value = lease
    provisioner._db = db
    provisioner._retire_managed_repository_agents_via_ssh = AsyncMock()

    released = (
        await provisioner.release_workspace("job")
        if owner_kind == "job"
        else await provisioner.release_thread_workspace(THREAD_ID)
    )

    assert released is True
    if owner_kind == "job":
        db.transition_docker_workspace_lease.assert_not_awaited()
    else:
        db.transition_docker_workspace_lease.assert_awaited_once_with(
            owner_kind="thread",
            owner_id=THREAD_ID,
            expected_lease_id="lease-thread",
            expected_statuses={"quarantined"},
            updates={"status": "quarantined"},
        )
    provisioner._retire_managed_repository_agents_via_ssh.assert_not_awaited()


@pytest.mark.asyncio
async def test_default_docker_retirement_uses_all_then_independent_zero_proof() -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    provisioner._run_pinned_workspace_command = AsyncMock(return_value=True)

    assert (
        await provisioner._retire_managed_repository_agents_via_ssh(
            "workspace-1", 30022
        )
        is True
    )
    command = provisioner._run_pinned_workspace_command.await_args.args[2]
    assert " all " in command
    assert " zero " in command
    assert command.index(" all ") < command.index(" zero ")
    assert "/home/agent-host/.ssh/srw-managed/sockets" in command


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_trusted_dev_cleanup_failure_quarantines(raises) -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    provisioner._trusted_dev_reuse = True
    db = AsyncMock()
    lease = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": "lease-dev",
    }
    db.get_job.return_value = {"context": {"workspace_container": lease}}
    transitions: list[dict[str, Any]] = []

    async def transition(**kwargs):
        transitions.append(kwargs["updates"])
        return {**lease, **kwargs["updates"]}

    db.transition_docker_workspace_lease.side_effect = transition
    provisioner._db = db
    if raises:
        provisioner._reset_workspace_via_ssh = AsyncMock(
            side_effect=RuntimeError("cleanup failed")
        )
    else:
        provisioner._reset_workspace_via_ssh = AsyncMock(return_value=False)

    assert await provisioner.release_workspace("job") is False
    assert transitions[-1] == {
        "status": "quarantined",
        "quarantine_reason": "dev_cleanup_failed",
    }


@pytest.mark.asyncio
async def test_trusted_dev_cleanup_requires_and_pins_inventory_identity(
    monkeypatch,
) -> None:
    from types import SimpleNamespace

    from orchestrator.services import docker_provisioner as module

    if module.asyncssh is None:
        pytest.skip("asyncssh is not installed in this unit-test environment")

    provisioner = module.DockerProvisioner()
    monkeypatch.setenv("SSH_KEY_PATH", "/tmp/id_ed25519")
    connect_calls: list[dict[str, Any]] = []

    async def should_not_connect(*args, **kwargs):
        connect_calls.append({"args": args, **kwargs})
        raise AssertionError("an unpinned cleanup target must not be contacted")

    monkeypatch.setattr(module.asyncssh, "connect", should_not_connect)
    assert await provisioner._reset_workspace_via_ssh("workspace-1", 30022) is False
    assert connect_calls == []

    class Connection:
        closed = False
        command = ""

        async def run(self, command, **kwargs):
            self.command = command
            assert kwargs == {"check": False, "timeout": 20}
            return SimpleNamespace(exit_status=0)

        def close(self):
            self.closed = True

        async def wait_closed(self):
            return None

    connection = Connection()

    async def connect(host, **kwargs):
        connect_calls.append({"host": host, **kwargs})
        return connection

    monkeypatch.setattr(module.asyncssh, "connect", connect)
    provisioner._workspace_fingerprints = {"workspace-1:30022": "SHA256:expected"}

    assert await provisioner._reset_workspace_via_ssh("workspace-1", 30022) is True
    options = connect_calls[-1]
    assert options["known_hosts"] == ((), (), (), (), (), (), ())
    assert options["known_hosts"]
    assert options["server_host_key_algs"] == ["ssh-ed25519"]
    validator = options["client_factory"]()
    wrong_key = SimpleNamespace(
        get_fingerprint=lambda algorithm: (
            "SHA256:wrong" if algorithm == "sha256" else ""
        )
    )
    assert (
        validator.validate_host_public_key("workspace-1", "127.0.0.1", 30022, wrong_key)
        is False
    )
    assert "stateless-process-zero" not in connection.command
    assert "starttime" in connection.command
    assert "ssh-agent" in connection.command
    assert "ssh-add -D" not in connection.command
    assert "rm -rf -- /home/agent-host/.ssh/srw-managed" in connection.command
    assert "test ! -e /home/agent-host/.ssh/srw-managed" in connection.command
    assert "rm -rf -- /home/agent-host/workspace" in connection.command
    assert "install -d -m 700 /home/agent-host/workspace" in connection.command
    assert "test ! -L /home/agent-host/workspace" in connection.command
    assert "find /home/agent-host/workspace -mindepth 1" in connection.command
    assert connection.closed is True


@pytest.mark.asyncio
async def test_docker_endpoint_is_paired_after_binding_and_fails_closed(
    monkeypatch,
) -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    monkeypatch.setenv("WORKSPACE_HOSTS", "workspace-1")
    monkeypatch.setenv(
        "WORKSPACE_HOST_KEY_FINGERPRINTS", "workspace-1:30022=SHA256:trusted"
    )
    provisioner = DockerProvisioner()
    db = AsyncMock()
    calls: list[str] = []
    db.acquire_docker_workspace_lease.return_value = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": "lease-success",
        "_docker_workspace_attested": True,
        "_canvas_workspace_generation": None,
    }

    async def bind(*args, **kwargs):
        del args, kwargs
        calls.append("bind")
        return {"workspace_generation": str(GENERATION), "changed": True}

    async def pair(**kwargs):
        calls.append("endpoint")
        return {
            **db.acquire_docker_workspace_lease.return_value,
            **kwargs["updates"],
        }

    db.bind_thread_workspace_backing.side_effect = bind
    db.transition_docker_workspace_lease.side_effect = pair
    provisioner.connect(db=db)
    result = await provisioner.assign_thread_workspace(THREAD_ID)
    assert result is not None
    assert calls == ["bind", "endpoint"]
    db.transition_docker_workspace_lease.assert_awaited_once_with(
        owner_kind="thread",
        owner_id=THREAD_ID,
        expected_lease_id="lease-success",
        expected_statuses={"ready"},
        updates={
            "status": "ready",
            "_canvas_workspace_generation": str(GENERATION),
        },
    )
    assert result["_canvas_workspace_generation"] == str(GENERATION)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["exception", "none"])
async def test_docker_binding_failure_quarantines_concrete_lease(
    monkeypatch, failure
) -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    monkeypatch.setenv("WORKSPACE_HOSTS", "workspace-1")
    monkeypatch.setenv(
        "WORKSPACE_HOST_KEY_FINGERPRINTS", "workspace-1:30022=SHA256:trusted"
    )
    provisioner = DockerProvisioner()
    db = AsyncMock()
    lease = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": f"lease-{failure}",
        "_docker_workspace_attested": True,
        "_canvas_workspace_generation": None,
    }
    db.acquire_docker_workspace_lease.return_value = lease
    if failure == "exception":
        db.bind_thread_workspace_backing.side_effect = RuntimeError("database down")
    elif failure == "none":
        db.bind_thread_workspace_backing.return_value = None
    quarantined = {
        **lease,
        "status": "quarantined",
    }
    db.transition_docker_workspace_lease.return_value = quarantined
    provisioner.connect(db=db)

    assert await provisioner.assign_thread_workspace(THREAD_ID) is None
    assert db.transition_docker_workspace_lease.await_args_list[-1].kwargs == {
        "owner_kind": "thread",
        "owner_id": THREAD_ID,
        "expected_lease_id": f"lease-{failure}",
        "expected_statuses": {"ready"},
        "updates": {
            "status": "quarantined",
            "_canvas_workspace_generation": None,
        },
    }
    assert db.transition_docker_workspace_lease.await_count == 1


@pytest.mark.asyncio
async def test_docker_canvas_pairing_loses_to_concurrent_release_cas(
    monkeypatch,
) -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    monkeypatch.setenv("WORKSPACE_HOSTS", "workspace-1")
    monkeypatch.setenv(
        "WORKSPACE_HOST_KEY_FINGERPRINTS", "workspace-1:30022=SHA256:trusted"
    )
    provisioner = DockerProvisioner()
    db = AsyncMock()
    lease = {
        "status": "ready",
        "host": "workspace-1",
        "port": 30022,
        "provisioner": "docker",
        "_docker_workspace_lease_id": "lease-race",
        "_docker_workspace_attested": True,
        "_canvas_workspace_generation": None,
    }
    db.acquire_docker_workspace_lease.return_value = lease
    db.bind_thread_workspace_backing.return_value = {
        "workspace_generation": str(GENERATION)
    }
    # A release claims ready -> releasing after binding but before endpoint
    # pairing. Both the pair CAS and best-effort quarantine must then lose.
    db.transition_docker_workspace_lease.side_effect = [None, None]
    provisioner.connect(db=db)

    assert await provisioner.assign_thread_workspace(THREAD_ID) is None
    first, second = db.transition_docker_workspace_lease.await_args_list
    assert first.kwargs == {
        "owner_kind": "thread",
        "owner_id": THREAD_ID,
        "expected_lease_id": "lease-race",
        "expected_statuses": {"ready"},
        "updates": {
            "status": "ready",
            "_canvas_workspace_generation": str(GENERATION),
        },
    }
    assert second.kwargs["updates"] == {
        "status": "quarantined",
        "_canvas_workspace_generation": None,
    }
    db.merge_thread_workspace_context.assert_not_awaited()


@pytest.mark.asyncio
async def test_docker_release_revokes_canvas_before_static_host_reset(
    monkeypatch,
) -> None:
    from unittest.mock import AsyncMock

    from orchestrator.services.docker_provisioner import DockerProvisioner

    provisioner = DockerProvisioner()
    db = AsyncMock()
    db.get_thread.return_value = {
        "metadata": {
            "workspace_container": {
                "status": "ready",
                "host": "workspace-1",
                "port": 30022,
                "provisioner": "docker",
                "_docker_workspace_lease_id": "lease-release",
                "_canvas_workspace_generation": str(GENERATION),
            }
        }
    }
    events: list[tuple[str, Any]] = []

    async def transition(**kwargs):
        assert kwargs["owner_id"] == THREAD_ID
        events.append(("db", kwargs["updates"].copy()))
        return {
            "host": "workspace-1",
            "port": 30022,
            "provisioner": "docker",
            "_docker_workspace_lease_id": "lease-release",
            **kwargs["updates"],
        }

    async def reset(host, port):
        events.append(("reset", (host, port)))
        return True

    async def record(owner_id, **kwargs):
        events.append(("receipt", (owner_id, kwargs)))
        return True

    db.transition_docker_workspace_lease.side_effect = transition
    db.record_docker_workspace_process_zero.side_effect = record
    provisioner._db = db
    provisioner._snapshot_service = None
    provisioner._trusted_dev_reuse = True
    monkeypatch.setattr(provisioner, "_reset_workspace_via_ssh", reset)

    assert await provisioner.release_thread_workspace(THREAD_ID) is True
    assert events[0] == (
        "db",
        {
            "status": "releasing",
            "quarantine_reason": "managed_repository_agent_retirement_claimed",
            "_canvas_workspace_generation": None,
        },
    )
    assert events[1] == ("reset", ("workspace-1", 30022))
    assert events[2] == (
        "receipt",
        (
            THREAD_ID,
            {"owner_kind": "thread", "lease_id": "lease-release"},
        ),
    )
    assert events[3][0] == "db"
    assert events[3][1]["status"] == "released"
    assert events[3][1]["_canvas_workspace_generation"] is None
    assert events[3][1]["_docker_workspace_trust_mode"] == "trusted_dev"
    assert events[3][1]["_docker_workspace_attested"] is False


def test_canvas_session_create_override_is_closed() -> None:
    import orchestrator.main

    with pytest.raises(orchestrator.main.HTTPException) as exc:
        orchestrator.main._validated_tool_overrides(
            {"tools": {"canvas": ["run_command"]}}
        )
    assert exc.value.status_code == 400

    # ``shell`` is no longer discarded — every category the request names is
    # honoured now (Defect 2). The canvas group is still closed against a
    # foreign name, which is the part this test exists for.
    accepted = orchestrator.main._validated_tool_overrides(
        {"tools": {"canvas": [], "shell": ["shell_execute"]}}
    )
    assert accepted == {"canvas": [], "shell": ["shell_execute"]}
    assert orchestrator.main._session_tool_group_disabled_markers(
        {"tools": {"canvas": []}}
    ) == {"_canvas_disabled": True}


def test_chart_internal_key_is_generated_and_reaches_agents() -> None:
    """MCP_INTERNAL_KEY must be minted, never defaulted to a known literal.

    The key authenticates delegated internal calls between the orchestrator,
    the MCP server, and provisioned agents. A hardcoded fallback anywhere in
    the chart would be a shared secret published in the repository, so the
    Secret template generates a random one when none is supplied (and
    preserves an already-installed value so a `helm upgrade` does not rotate
    it out from under running pods).
    """
    root = Path(__file__).resolve().parents[1]
    secret = (root / "helm/templates/secret.yaml").read_text()

    assert "randAlphaNum 48" in secret
    assert "MCP_INTERNAL_KEY: {{ $internalKey | quote }}" in secret

    for relative_path in (
        "helm/templates/orchestrator/deployment.yaml",
        "helm/templates/mcp/deployment.yaml",
        "helm/templates/agent/stateless-deployment.yaml",
    ):
        source = (root / relative_path).read_text()
        assert "- name: MCP_INTERNAL_KEY" in source, relative_path
        assert "key: MCP_INTERNAL_KEY" in source, relative_path
        assert 'include "srw.secretName"' in source, relative_path

    for path in (root / "helm").rglob("*.yaml"):
        assert "dev-internal-key" not in path.read_text(), path
