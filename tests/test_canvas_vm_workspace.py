"""The file Canvas and the agent's Canvas grant on a same-cluster VM (WP7).

A VM Session reads Canvas files from its VM through the pinned SSH pool, with
a controller proof before and after every read; presentations survive a VM
restore onto the kept rootdisk like a container's survive a pod recreate on
the same volume; writes stay refused (no durable VM write authority yet); a
VM on the tailnet gets no Canvas at all.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import pytest

from orchestrator.services import canvas_files
from orchestrator.services import canvas_ssh
from orchestrator.services import thread_workspace_delivery as delivery
from orchestrator.services import vm_provisioner as vm_provisioner_module
from orchestrator.services.canvas import (
    CanvasRecord,
    WorkspaceFileSource,
    canonical_source_fingerprint,
)
from orchestrator.services.canvas_files import (
    CanvasFileError,
    RawWorkspaceFile,
    ThreadWorkspaceFileGateway,
)
from tests._canvas_vm_fixtures import (
    RESTORED_FINGERPRINT,
    RESTORED_HOST,
    VM_FINGERPRINT,
    VM_HOST,
    VM_ROOTDISK,
    VM_THREAD_ID,
    FakeVMProvisioner,
    restored_vm_context,
    vm_context,
    vm_thread,
)

NOW = datetime(2026, 11, 21, tzinfo=timezone.utc)
BODY = b"# Report\n"


class _Magic:
    @staticmethod
    def from_buffer(data: bytes, mime: bool = False) -> str:
        del data, mime
        return "text/plain"


class _Row:
    """A mutable thread row behind the gateway's thread loader."""

    def __init__(self, thread: dict[str, Any]) -> None:
        self.thread = thread

    async def get_thread(self, thread_id: str) -> dict[str, Any]:
        assert thread_id == VM_THREAD_ID
        return self.thread


class _Pool:
    def __init__(self, events: list) -> None:
        self.events = events
        self.checkouts: list[dict[str, Any]] = []

    @asynccontextmanager
    async def checkout(self, **kwargs: Any):
        self.checkouts.append(kwargs)
        self.events.append("checkout")
        yield object()

    async def evict_generation(self, thread_id: str, generation: UUID) -> None:
        self.events.append(("evict", thread_id, generation))


@pytest.fixture()
def vm_file_canvas(monkeypatch):
    monkeypatch.setenv("VM_MODE", "same-cluster")
    monkeypatch.setattr(canvas_files, "magic", _Magic)
    monkeypatch.setattr(canvas_files, "resolve_ssh_key_path", lambda: "/tmp/key")
    if canvas_files.asyncssh is None:  # pragma: no cover - dependency guard
        monkeypatch.setattr(canvas_files, "asyncssh", object())
    row = _Row(vm_thread())
    events: list = []
    pool = _Pool(events)
    provisioner = FakeVMProvisioner(row.thread)
    verdicts: list[str] = []

    real_attest = canvas_ssh.attest_vm_workspace_target

    async def attest(target):
        events.append("attest")
        if verdicts:
            return verdicts.pop(0)
        return await real_attest(target)

    async def read_file(sftp, *, root, path, read_limit=None):
        del sftp, read_limit
        events.append(("read", root, path))
        return RawWorkspaceFile(BODY, NOW)

    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", provisioner)
    monkeypatch.setattr(canvas_files, "attest_vm_workspace_target", attest)
    monkeypatch.setattr(canvas_files, "_SFTP_POOL", pool)
    monkeypatch.setattr(canvas_files, "_read_sftp_workspace_file", read_file)
    gateway = ThreadWorkspaceFileGateway(thread_loader=row.get_thread)
    return row, events, pool, provisioner, verdicts, gateway


def _record(generation: UUID, source_version: str) -> CanvasRecord:
    source = WorkspaceFileSource(
        path="output/report.md", workspace_generation=generation
    )
    return CanvasRecord(
        thread_id=VM_THREAD_ID,
        canvas_id="main",
        source=source,
        title="Report",
        renderer="markdown",
        editable=True,
        alt_text=None,
        presentation_revision=1,
        source_fingerprint=canonical_source_fingerprint(source),
        source_version=source_version,
        origin_generation=None,
        created_at=NOW,
        updated_at=NOW,
    )


@pytest.mark.asyncio
async def test_vm_presentation_reads_the_vm_between_two_controller_proofs(
    vm_file_canvas,
) -> None:
    row, events, pool, provisioner, _verdicts, gateway = vm_file_canvas

    generation, validated = await gateway.validate_for_presentation(
        row.thread, "output/report.md"
    )

    assert generation == canvas_ssh.vm_canvas_generation(VM_THREAD_ID, VM_ROOTDISK)
    assert validated.data == BODY
    assert events == [
        "attest",
        "checkout",
        ("read", canvas_files.REMOTE_WORKSPACE_ROOT, "output/report.md"),
        "attest",
    ]
    assert pool.checkouts == [
        {
            "thread_id": VM_THREAD_ID,
            "generation": generation,
            "host": VM_HOST,
            "port": 22,
            "fingerprint": VM_FINGERPRINT,
            "key_path": "/tmp/key",
        }
    ]
    assert provisioner.calls == [(VM_THREAD_ID, "thread"), (VM_THREAD_ID, "thread")]


@pytest.mark.asyncio
async def test_a_presentation_survives_a_restore_onto_the_kept_rootdisk(
    vm_file_canvas,
) -> None:
    row, _events, pool, _provisioner, _verdicts, gateway = vm_file_canvas
    generation, validated = await gateway.validate_for_presentation(
        row.thread, "output/report.md"
    )
    record = _record(generation, validated.source_version)

    # Suspend, then restore: a new VM, launcher, address and host key on the
    # same disk. The presentation is the same file and reads again, through
    # the new endpoint and pin.
    row.thread["metadata"]["vm"] = restored_vm_context()
    materialized = await gateway.materialize_current(row.thread, record)

    assert materialized.source_version == record.source_version
    assert pool.checkouts[-1]["host"] == RESTORED_HOST
    assert pool.checkouts[-1]["fingerprint"] == RESTORED_FINGERPRINT
    assert pool.checkouts[-1]["generation"] == generation

    # A new disk is a new file namespace, as for a container's new volume.
    row.thread["metadata"]["vm"] = restored_vm_context(
        rootdisk_pvc_uid="abababab-abab-4bab-8bab-abababababab"
    )
    with pytest.raises(CanvasFileError) as error:
        await gateway.materialize_current(row.thread, record)
    assert error.value.code == "workspace_generation_changed"


@pytest.mark.asyncio
async def test_a_suspended_vm_presentation_is_unavailable(vm_file_canvas) -> None:
    row, events, _pool, _provisioner, _verdicts, gateway = vm_file_canvas
    generation, validated = await gateway.validate_for_presentation(
        row.thread, "output/report.md"
    )
    record = _record(generation, validated.source_version)
    events.clear()

    row.thread["metadata"]["vm"]["status"] = "suspending"
    with pytest.raises(CanvasFileError) as error:
        await gateway.materialize_current(row.thread, record)

    # A snapshot-fallback code: the Canvas shows the remembered bytes.
    assert error.value.code in {"workspace_unavailable", "workspace_generation_changed"}
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("verdicts", "code", "read"),
    [
        (["changed"], "workspace_generation_changed", False),
        (["unknown"], "workspace_unavailable", False),
        (["current", "changed"], "workspace_generation_changed", True),
        (["current", "unknown"], "workspace_unavailable", True),
    ],
)
async def test_bytes_from_an_unproven_vm_are_never_served(
    vm_file_canvas, verdicts, code, read
) -> None:
    row, events, _pool, _provisioner, scripted, gateway = vm_file_canvas
    scripted.extend(verdicts)

    with pytest.raises(CanvasFileError) as error:
        await gateway.validate_for_presentation(row.thread, "output/report.md")

    assert error.value.code == code
    assert any(isinstance(event, tuple) and event[0] == "read" for event in events) is (
        read
    )


@pytest.mark.asyncio
async def test_launcher_replaced_during_the_read_is_a_change(
    vm_file_canvas, monkeypatch
) -> None:
    row, events, _pool, _provisioner, _verdicts, gateway = vm_file_canvas

    async def read_then_replace(sftp, *, root, path, read_limit=None):
        del sftp, root, path, read_limit
        row.thread["metadata"]["vm"]["active_pod_uid"] = (
            "13131313-1313-4313-8313-131313131313"
        )
        return RawWorkspaceFile(BODY, NOW)

    monkeypatch.setattr(canvas_files, "_read_sftp_workspace_file", read_then_replace)
    with pytest.raises(CanvasFileError) as error:
        await gateway.validate_for_presentation(row.thread, "output/report.md")

    assert error.value.code == "workspace_generation_changed"
    assert any(event[0] == "evict" for event in events if isinstance(event, tuple))


@pytest.mark.asyncio
async def test_virtual_label_with_a_ready_vm_reads_the_vm(vm_file_canvas) -> None:
    row, events, _pool, _provisioner, _verdicts, gateway = vm_file_canvas
    # Upgraded from Virtual: the declared backend never changes.
    row.thread["metadata"]["config_override"]["workspace"]["backend"] = "virtual"

    async def no_object_store(*args, **kwargs):
        raise AssertionError((args, kwargs))

    gateway._virtual_loader = no_object_store
    path, raw = await gateway.read_download(row.thread, "output/report.md")

    assert (path, raw.data) == ("output/report.md", BODY)
    assert "checkout" in events


@pytest.mark.asyncio
async def test_vm_presentations_are_read_only(vm_file_canvas) -> None:
    row, events, _pool, _provisioner, _verdicts, gateway = vm_file_canvas
    generation, validated = await gateway.validate_for_presentation(
        row.thread, "output/report.md"
    )
    record = _record(generation, validated.source_version)
    events.clear()

    assert gateway.supports_editing(row.thread, record) is False
    with pytest.raises(CanvasFileError) as error:
        await canvas_files._write_remote_default(
            row.thread, "output/report.md", generation, b"# Edited\n"
        )

    assert error.value.code == "canvas_not_editable"
    assert events == []


def test_container_editing_is_unchanged(monkeypatch) -> None:
    monkeypatch.setattr(canvas_files, "resolve_ssh_key_path", lambda: "/tmp/key")
    container = {
        "id": VM_THREAD_ID,
        "metadata": {
            "config_override": {"workspace": {"backend": "sandbox"}},
            "_workspace_binding": {
                "generation": "11111111-aaaa-4aaa-8aaa-111111111111",
                "kind": "remote",
                "backing_id": "k8s-pvc:srw:x",
                "ssh_host_key_fingerprint": "SHA256:test",
            },
            "workspace_container": {
                "status": "ready",
                "pod_ip": "10.42.0.5",
                "port": 30022,
                "_canvas_workspace_generation": "11111111-aaaa-4aaa-8aaa-111111111111",
            },
        },
    }
    assert ThreadWorkspaceFileGateway().supports_editing(container) is (
        canvas_files.asyncssh is not None
    )


# ---------------------------------------------------------------------------
# What the agent is told at attach
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("mode", "browser", "vm", "expected"),
    [
        ("same-cluster", "true", vm_context(), (True, False, True)),
        ("same-cluster", "false", vm_context(), (True, False, False)),
        # The tailnet: no route from the orchestrator, so no Canvas at all.
        ("external", "true", vm_context(ssh_host="100.64.1.2"), (False, False, False)),
        ("off", "true", vm_context(), (False, False, False)),
        # Not controller-authenticated, or suspension closed remote I/O.
        (
            "same-cluster",
            "true",
            vm_context(identity_authenticated=False),
            (False, False, False),
        ),
        (
            "same-cluster",
            "true",
            vm_context(_suspend_remote_io_closed="lease-1"),
            (False, False, False),
        ),
    ],
)
def test_agent_vm_canvas_grant(monkeypatch, mode, browser, vm, expected) -> None:
    monkeypatch.setenv("VM_MODE", mode)
    monkeypatch.setenv("CANVAS_SHARED_BROWSER_ENABLED", browser)
    # The live-app proxy has no VM path, whatever its flag says.
    monkeypatch.setenv("CANVAS_LIVE_PREVIEW_ENABLED", "true")
    metadata = vm_thread(vm)["metadata"]

    assert (
        delivery.agent_canvas_workspace_capabilities(
            metadata, metadata["workspace_container"], vm
        )
        == expected
    )


class _RepinService:
    """Runs the real re-pin verifier against a given (locked) row."""

    def __init__(self, record: CanvasRecord, locked_thread: dict[str, Any]) -> None:
        self.record = record
        self.locked_thread = locked_thread
        self.verified: list[Any] = []

    async def repin_workspace_generation(
        self, thread_id, *, expected_source_version, verifier
    ):
        from orchestrator.services.canvas import CanvasMutation

        del thread_id, expected_source_version
        self.verified.append(await verifier(self.record, self.locked_thread))
        return CanvasMutation(changed=False, record=self.record)


@pytest.mark.asyncio
async def test_vm_restore_needs_no_repin_on_the_kept_disk_and_repins_a_new_one(
    vm_file_canvas, monkeypatch
) -> None:
    from orchestrator.routers import canvases

    row, events, _pool, _provisioner, _verdicts, gateway = vm_file_canvas
    generation, validated = await gateway.validate_for_presentation(
        row.thread, "output/report.md"
    )
    record = _record(generation, validated.source_version)
    canvases._REPIN_ATTEMPTED.clear()
    monkeypatch.setattr(canvases, "_get_file_gateway", lambda db=None: gateway)

    # Kept rootdisk: same generation, so the state read touches nothing.
    row.thread["metadata"]["vm"] = restored_vm_context()
    events.clear()
    kept = _RepinService(record, row.thread)
    monkeypatch.setattr(canvases, "_get_canvas_service", lambda db: kept)
    assert await canvases._maybe_repin(object(), VM_THREAD_ID, row.thread, record) is (
        record
    )
    assert kept.verified == [] and events == []

    # A snapshot restore onto a new disk: the same bytes re-bind the
    # presentation to the new generation, read through the restored VM.
    row.thread["metadata"]["vm"] = restored_vm_context(
        rootdisk_pvc_uid="abababab-abab-4bab-8bab-abababababab"
    )
    fresh = _RepinService(record, row.thread)
    monkeypatch.setattr(canvases, "_get_canvas_service", lambda db: fresh)
    await canvases._maybe_repin(object(), VM_THREAD_ID, row.thread, record)

    assert fresh.verified == [
        canvas_ssh.vm_canvas_generation(
            VM_THREAD_ID, "abababab-abab-4bab-8bab-abababababab"
        )
    ]
    assert "attest" in events and "checkout" in events
