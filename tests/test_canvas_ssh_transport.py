"""Focused shared SSH transport contracts for Dynamic Canvas Slice 3A."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import pytest

from orchestrator.services import canvas_ssh
from orchestrator.services.canvas_ssh import (
    CANVAS_LOOPBACK_HOST,
    CanvasDirectChannelUnavailable,
    CanvasSSHError,
    PinnedSSHCommandResult,
    PinnedSSHTransportPool,
    RemoteWorkspaceTarget,
)

THREAD_ID = "a3333333-3333-3333-3333-333333333333"
GENERATION = UUID("11111111-aaaa-4aaa-8aaa-111111111111")
NEXT_GENERATION = UUID("22222222-bbbb-4bbb-8bbb-222222222222")


def _thread(
    *,
    generation: UUID = GENERATION,
    host: str = "workspace.test",
    fingerprint: str = "SHA256:test",
) -> dict[str, Any]:
    return {
        "id": THREAD_ID,
        "metadata": {
            "_workspace_binding": {
                "generation": str(generation),
                "kind": "remote",
                "backing_id": "workspace-a",
                "ssh_host_key_fingerprint": fingerprint,
            },
            "workspace_container": {
                "status": "ready",
                "host": host,
                "port": 30022,
                "_canvas_workspace_generation": str(generation),
            },
        },
    }


class _Writer:
    def __init__(self) -> None:
        self.closed = False
        self.waited = False

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        self.waited = True


class _StallingWriter(_Writer):
    async def wait_closed(self) -> None:
        self.waited = True
        await asyncio.Event().wait()


class _Connection:
    def __init__(self) -> None:
        self.closed = False
        self.remote_closed = False
        self.close_calls = 0
        self.waited = False
        self.open_error: BaseException | None = None
        self.open_calls: list[tuple[str, int]] = []
        self.writers: list[_Writer] = []
        self.processes: list[_Process] = []
        self.process_error: BaseException | None = None
        self.commands: list[str] = []

    def is_closed(self) -> bool:
        return self.closed or self.remote_closed

    def close(self) -> None:
        self.close_calls += 1
        self.closed = True

    async def wait_closed(self) -> None:
        self.waited = True

    async def open_connection(self, host: str, port: int):
        self.open_calls.append((host, port))
        if self.open_error is not None:
            raise self.open_error
        writer = _Writer()
        self.writers.append(writer)
        return object(), writer

    async def create_process(self, command: str, *, encoding=None):
        assert encoding is None
        self.commands.append(command)
        if self.process_error is not None:
            raise self.process_error
        return self.processes.pop(0)


class _Reader:
    def __init__(self, data: bytes = b"", *, stall: bool = False) -> None:
        self._data = bytearray(data)
        self._stall = stall

    async def read(self, size: int) -> bytes:
        if self._stall:
            await asyncio.Event().wait()
        if not self._data:
            return b""
        result = bytes(self._data[:size])
        del self._data[:size]
        return result


class _Process:
    def __init__(
        self,
        *,
        stdout: bytes = b"",
        stderr: bytes = b"",
        exit_status: int = 0,
        stall: bool = False,
    ) -> None:
        self.stdout = _Reader(stdout, stall=stall)
        self.stderr = _Reader(stderr, stall=stall)
        self.exit_status = exit_status
        self.terminated = False
        self.waited = False

    def terminate(self) -> None:
        self.terminated = True
        self.stdout._stall = False
        self.stderr._stall = False

    async def wait_closed(self) -> None:
        self.waited = True


def _target(generation: UUID = GENERATION) -> RemoteWorkspaceTarget:
    return RemoteWorkspaceTarget(
        thread_id=THREAD_ID,
        generation=generation,
        host="workspace.test",
        port=30022,
        fingerprint="SHA256:test",
    )


@pytest.mark.asyncio
async def test_direct_channel_teardown_is_bounded(monkeypatch) -> None:
    writer = _StallingWriter()
    connection = _Connection()

    async def open_connection(host: str, port: int):
        connection.open_calls.append((host, port))
        return object(), writer

    connection.open_connection = open_connection  # type: ignore[method-assign]
    pool = PinnedSSHTransportPool(idle_timeout=60)
    entry = canvas_ssh._PooledSSHTransport(  # noqa: SLF001
        target=_target(),
        connection=connection,
        last_used=0,
        leases=1,
    )

    async def checkout(**kwargs):
        del kwargs
        yield connection

    from contextlib import asynccontextmanager

    monkeypatch.setattr(pool, "checkout", asynccontextmanager(checkout))
    monkeypatch.setattr(canvas_ssh, "DEFAULT_SSH_CLOSE_TIMEOUT_SECONDS", 0.01)
    async with asyncio.timeout(0.5):
        async with pool.open_loopback_connection(
            target=entry.target,
            destination_port=8501,
            key_path="/tmp/key",
            generation_resolver=lambda: _resolved_thread(),
        ):
            pass
    assert writer.closed
    assert writer.waited


async def _resolved_thread() -> dict[str, Any]:
    return _thread()


@pytest.mark.asyncio
async def test_run_command_uses_pinned_transport_and_revalidates_identity(
    monkeypatch,
) -> None:
    connection = _Connection()
    connection.processes.append(
        _Process(stdout=b"ready\n", stderr=b"warning\n", exit_status=0)
    )
    connect_calls = 0

    async def connect(*args: Any, **kwargs: Any):
        nonlocal connect_calls
        del args, kwargs
        connect_calls += 1
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)
    resolver_calls = 0

    async def current() -> dict[str, Any]:
        nonlocal resolver_calls
        resolver_calls += 1
        return _thread()

    result = await pool.run_command(
        target=_target(),
        command="browser-exec stream_info --json '{}'",
        key_path="/tmp/id_ed25519",
        generation_resolver=current,
        timeout=1,
        max_output_bytes=64 * 1024,
    )

    assert result == PinnedSSHCommandResult(
        exit_status=0,
        stdout=b"ready\n",
        stderr=b"warning\n",
    )
    assert connection.commands == ["browser-exec stream_info --json '{}'"]
    assert resolver_calls == 2
    assert connect_calls == 1
    await pool.close_all()


@pytest.mark.asyncio
async def test_run_command_rejects_post_command_generation_change(monkeypatch) -> None:
    connection = _Connection()
    connection.processes.append(_Process(stdout=b"ready\n"))

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)
    calls = 0

    async def rotated() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _thread() if calls == 1 else _thread(generation=NEXT_GENERATION)

    with pytest.raises(CanvasSSHError) as error:
        await pool.run_command(
            target=_target(),
            command="true",
            key_path="/tmp/key",
            generation_resolver=rotated,
            timeout=1,
            max_output_bytes=64 * 1024,
        )

    assert error.value.code == "workspace_generation_changed"
    assert connection.closed is False
    await pool.close_all()


@pytest.mark.asyncio
async def test_run_command_rejects_generation_change_before_process(
    monkeypatch,
) -> None:
    connection = _Connection()

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)

    async def rotated() -> dict[str, Any]:
        return _thread(generation=NEXT_GENERATION)

    with pytest.raises(CanvasSSHError) as error:
        await pool.run_command(
            target=_target(),
            command="true",
            key_path="/tmp/key",
            generation_resolver=rotated,
            timeout=1,
            max_output_bytes=64 * 1024,
        )

    assert error.value.code == "workspace_generation_changed"
    assert connection.commands == []
    await pool.close_all()


@pytest.mark.asyncio
async def test_run_command_nonzero_is_channel_scoped_and_transport_is_reused(
    monkeypatch,
) -> None:
    connection = _Connection()
    connection.processes.extend(
        [
            _Process(stdout=b"out", stderr=b"failed", exit_status=7),
            _Process(stdout=b"ok", exit_status=0),
        ]
    )
    connect_calls = 0

    async def connect(*args: Any, **kwargs: Any):
        nonlocal connect_calls
        del args, kwargs
        connect_calls += 1
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)
    kwargs = {
        "target": _target(),
        "key_path": "/tmp/key",
        "generation_resolver": _resolved_thread,
        "timeout": 1,
        "max_output_bytes": 64 * 1024,
    }

    failed = await pool.run_command(command="false", **kwargs)
    succeeded = await pool.run_command(command="true", **kwargs)

    assert failed.exit_status == 7
    assert failed.stderr == b"failed"
    assert succeeded.stdout == b"ok"
    assert connect_calls == 1
    assert connection.closed is False
    await pool.close_all()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_run_command_timeout_or_cancellation_terminates_process(
    monkeypatch, cancel: bool
) -> None:
    connection = _Connection()
    process = _Process(stall=True)
    connection.processes.append(process)

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)
    task = asyncio.create_task(
        pool.run_command(
            target=_target(),
            command="sleep forever",
            key_path="/tmp/key",
            generation_resolver=_resolved_thread,
            timeout=0.01 if not cancel else 60,
            max_output_bytes=64 * 1024,
        )
    )
    if cancel:
        while not connection.commands:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(CanvasSSHError) as error:
            await task
        assert error.value.status_code == 504

    assert process.terminated is True
    assert process.waited is True
    await pool.close_all()


@pytest.mark.asyncio
async def test_run_command_bounds_combined_output(monkeypatch) -> None:
    connection = _Connection()
    process = _Process(stdout=b"a" * 32768, stderr=b"b" * 32769)
    connection.processes.append(process)

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)

    with pytest.raises(CanvasSSHError) as error:
        await pool.run_command(
            target=_target(),
            command="noisy",
            key_path="/tmp/key",
            generation_resolver=_resolved_thread,
            timeout=1,
            max_output_bytes=64 * 1024,
        )

    assert error.value.code == "workspace_command_output_too_large"
    assert process.terminated is True
    assert process.waited is True
    assert connection.closed is False
    await pool.close_all()


@pytest.mark.asyncio
async def test_run_command_invalidates_a_broken_transport(monkeypatch) -> None:
    connection = _Connection()
    connection.remote_closed = True
    connection.process_error = OSError("transport lost")

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        connection.remote_closed = False
        return connection

    original_create = connection.create_process

    async def broken_create(*args: Any, **kwargs: Any):
        connection.remote_closed = True
        return await original_create(*args, **kwargs)

    connection.create_process = broken_create  # type: ignore[method-assign]
    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)

    with pytest.raises(CanvasSSHError) as error:
        await pool.run_command(
            target=_target(),
            command="true",
            key_path="/tmp/key",
            generation_resolver=_resolved_thread,
            timeout=1,
            max_output_bytes=64 * 1024,
        )

    assert error.value.code == "workspace_unavailable"
    assert connection.closed is True
    assert pool._entries == {}


def test_pinned_client_rejects_host_key_mismatch() -> None:
    class Key:
        def __init__(self, fingerprint: str) -> None:
            self.fingerprint = fingerprint

        def get_fingerprint(self, algorithm: str) -> str:
            assert algorithm == "sha256"
            return self.fingerprint

    client = canvas_ssh.PinnedSSHClient("SHA256:expected")

    assert client.validate_host_public_key(None, None, None, Key("SHA256:expected"))
    assert not client.validate_host_public_key(
        None, None, None, Key("SHA256:unexpected")
    )


@pytest.mark.asyncio
async def test_direct_channel_uses_exact_pin_and_fixed_loopback(monkeypatch) -> None:
    if canvas_ssh.asyncssh is None:
        pytest.skip("asyncssh is unavailable")
    captured: dict[str, Any] = {}
    connection = _Connection()

    async def connect(host: str, **kwargs: Any):
        captured.update(host=host, **kwargs)
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)

    async def current() -> dict[str, Any]:
        return _thread()

    async with pool.open_loopback_connection(
        target=_target(),
        destination_port=8501,
        key_path="/tmp/id_ed25519",
        generation_resolver=current,
    ):
        pass

    assert captured["host"] == "workspace.test"
    assert captured["known_hosts"] == ((), (), (), (), (), (), ())
    assert captured["known_hosts"]
    assert captured["server_host_key_algs"] == ["ssh-ed25519"]
    assert connection.open_calls == [(CANVAS_LOOPBACK_HOST, 8501)]
    assert connection.writers[0].closed is True
    assert connection.writers[0].waited is True
    await pool.close_all()


@pytest.mark.asyncio
async def test_cold_concurrent_checkouts_singleflight_one_handshake(
    monkeypatch,
) -> None:
    connection = _Connection()
    connect_calls = 0
    started = asyncio.Event()
    release = asyncio.Event()

    async def connect(*args: Any, **kwargs: Any):
        nonlocal connect_calls
        del args, kwargs
        connect_calls += 1
        started.set()
        await release.wait()
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)

    async def use_transport() -> None:
        async with pool.checkout(target=_target(), key_path="/tmp/key") as current:
            assert current is connection

    tasks = [asyncio.create_task(use_transport()) for _ in range(10)]
    await started.wait()
    release.set()
    await asyncio.gather(*tasks)

    assert connect_calls == 1
    await pool.close_all()


@pytest.mark.asyncio
async def test_cancelled_candidate_before_insertion_is_closed(monkeypatch) -> None:
    connection = _Connection()
    started = asyncio.Event()
    release_connect = asyncio.Event()

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        started.set()
        await release_connect.wait()
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)

    async def use_transport() -> None:
        async with pool.checkout(target=_target(), key_path="/tmp/key"):
            pass

    task = asyncio.create_task(use_transport())
    await started.wait()
    await pool._lock.acquire()
    release_connect.set()
    await asyncio.sleep(0)
    task.cancel()
    pool._lock.release()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert connection.closed is True
    assert pool._entries == {}


@pytest.mark.asyncio
async def test_failed_reconnect_closes_removed_stale_entry(monkeypatch) -> None:
    connection = _Connection()
    calls = 0

    async def connect(*args: Any, **kwargs: Any):
        nonlocal calls
        del args, kwargs
        calls += 1
        if calls == 1:
            return connection
        raise OSError("unreachable")

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)
    async with pool.checkout(target=_target(), key_path="/tmp/key"):
        pass
    connection.remote_closed = True

    with pytest.raises(CanvasSSHError) as error:
        async with pool.checkout(target=_target(), key_path="/tmp/key"):
            pass

    assert error.value.code == "workspace_unavailable"
    assert connection.close_calls == 1
    assert connection.waited is True


@pytest.mark.asyncio
async def test_domain_error_does_not_poison_concurrent_transport(monkeypatch) -> None:
    connection = _Connection()
    connect_calls = 0

    async def connect(*args: Any, **kwargs: Any):
        nonlocal connect_calls
        del args, kwargs
        connect_calls += 1
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)
    first = pool.checkout(target=_target(), key_path="/tmp/key")
    await first.__aenter__()

    with pytest.raises(ValueError, match="domain failure"):
        async with pool.checkout(target=_target(), key_path="/tmp/key"):
            raise ValueError("domain failure")

    assert connection.closed is False
    async with pool.checkout(target=_target(), key_path="/tmp/key") as reused:
        assert reused is connection
    assert connect_calls == 1
    await first.__aexit__(None, None, None)
    await pool.close_all()


@pytest.mark.asyncio
async def test_only_open_connect_failed_reports_starting(monkeypatch) -> None:
    if canvas_ssh.asyncssh is None:
        pytest.skip("asyncssh is unavailable")
    connection = _Connection()

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)

    async def current() -> dict[str, Any]:
        return _thread()

    connection.open_error = canvas_ssh.asyncssh.ChannelOpenError(
        canvas_ssh.asyncssh.OPEN_CONNECT_FAILED, "closed"
    )
    with pytest.raises(CanvasDirectChannelUnavailable):
        async with pool.open_loopback_connection(
            target=_target(),
            destination_port=8501,
            key_path="/tmp/key",
            generation_resolver=current,
        ):
            pass

    connection.open_error = canvas_ssh.asyncssh.ChannelOpenError(
        canvas_ssh.asyncssh.OPEN_ADMINISTRATIVELY_PROHIBITED, "denied"
    )
    with pytest.raises(CanvasSSHError) as denied:
        async with pool.open_loopback_connection(
            target=_target(),
            destination_port=8501,
            key_path="/tmp/key",
            generation_resolver=current,
        ):
            pass
    assert denied.value.code == "workspace_unavailable"

    connection.open_error = OSError("transport broke")
    with pytest.raises(CanvasSSHError) as transport:
        async with pool.open_loopback_connection(
            target=_target(),
            destination_port=8501,
            key_path="/tmp/key",
            generation_resolver=current,
        ):
            pass
    assert transport.value.code == "workspace_unavailable"
    assert connection.closed is True
    await pool.close_all()


@pytest.mark.asyncio
async def test_post_open_identity_failure_always_closes_writer(monkeypatch) -> None:
    connection = _Connection()

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        return connection

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=60)
    calls = 0

    async def rotated() -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return _thread() if calls == 1 else _thread(generation=NEXT_GENERATION)

    with pytest.raises(CanvasSSHError) as error:
        async with pool.open_loopback_connection(
            target=_target(),
            destination_port=8501,
            key_path="/tmp/key",
            generation_resolver=rotated,
        ):
            pass

    assert error.value.code == "workspace_generation_changed"
    assert connection.writers[0].closed is True
    assert connection.writers[0].waited is True
    await pool.close_all()


@pytest.mark.asyncio
async def test_idle_and_generation_eviction_close_only_intended_entries(
    monkeypatch,
) -> None:
    connections = [_Connection(), _Connection(), _Connection()]

    async def connect(*args: Any, **kwargs: Any):
        del args, kwargs
        return connections.pop(0)

    monkeypatch.setattr(canvas_ssh.asyncssh, "connect", connect)
    pool = PinnedSSHTransportPool(idle_timeout=0.01)
    old = pool.checkout(target=_target(), key_path="/tmp/key")
    new = pool.checkout(target=_target(NEXT_GENERATION), key_path="/tmp/key")
    old_connection = await old.__aenter__()
    new_connection = await new.__aenter__()

    await pool.evict_generation(THREAD_ID, GENERATION)
    assert old_connection.closed is True
    assert new_connection.closed is False
    await old.__aexit__(None, None, None)
    await new.__aexit__(None, None, None)
    await asyncio.sleep(0.03)
    assert new_connection.closed is True

    thread_pool = PinnedSSHTransportPool()
    assert thread_pool._idle_timeout == 60.0
    active = thread_pool.checkout(target=_target(), key_path="/tmp/key")
    active_connection = await active.__aenter__()
    await thread_pool.evict_thread(THREAD_ID)
    assert active_connection.closed is True
    await active.__aexit__(None, None, None)


# ---------------------------------------------------------------------------
# Same-cluster VM identity (WP7)
# ---------------------------------------------------------------------------

from tests._canvas_vm_fixtures import (  # noqa: E402
    RESTORED_FINGERPRINT,
    RESTORED_HOST,
    RESTORED_LAUNCHER,
    RESTORED_PROVISION_GENERATION,
    VM_FINGERPRINT,
    VM_HOST,
    VM_LAUNCHER,
    VM_PROVISION_GENERATION,
    VM_ROOTDISK,
    VM_THREAD_ID,
    FakeVMProvisioner,
    proof_for,
    restored_vm_context,
    vm_context,
    vm_thread,
)
from orchestrator.services import vm_provisioner as vm_provisioner_module  # noqa: E402
from shared.workspace_recovery import WorkspaceRecoveryCode  # noqa: E402


def test_vm_identity_is_the_complete_authenticated_tuple() -> None:
    identity = canvas_ssh.vm_canvas_identity(vm_context())

    assert identity == canvas_ssh.VMCanvasIdentity(
        provision_generation=VM_PROVISION_GENERATION,
        vm_uid="vm-uid",
        launcher_pod_uid=VM_LAUNCHER,
        rootdisk_pvc_uid=VM_ROOTDISK,
        host=VM_HOST,
        port=22,
        fingerprint=VM_FINGERPRINT,
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"status": "suspending"},
        {"status": "provisioning"},
        {"_suspend_remote_io_closed": "lease-1"},
        {"identity_authenticated": False},
        {"identity_authenticated": "true"},
        {"ssh_ready_source": "daemon"},
        {"ssh_ready_source": None},
        {"identity_provision_generation": RESTORED_PROVISION_GENERATION},
        {"provision_generation": "not-a-uuid"},
        {"active_pod_uid": None},
        {"active_pod_uid": "launcher"},
        {"vm_uid": ""},
        {"vm_uid": "vm uid"},
        {"rootdisk_pvc_uid": None},
        {"ssh_host": None},
        {"ssh_host": "10.42.1.23 "},
        {"ssh_port": True},
        {"ssh_port": 0},
        {"ssh_port": 70000},
        {"ssh_port": "22x"},
        {"ssh_host_key_fingerprint": None},
        {"ssh_host_key_fingerprint": "MD5:aa"},
        {"ssh_host_key_fingerprint": "SHA256:a b"},
    ],
)
def test_incomplete_vm_identity_resolves_to_nothing(overrides) -> None:
    assert canvas_ssh.vm_canvas_identity(vm_context(**overrides)) is None
    thread = vm_thread(vm_context(**overrides))
    if canvas_ssh.remote_target_is_vm_backed(thread):
        with pytest.raises(CanvasSSHError) as error:
            canvas_ssh.canvas_workspace_generation(thread)
        assert error.value.code == "workspace_unavailable"


def test_vm_target_carries_the_runtime_and_pins_the_guest_key() -> None:
    thread = vm_thread()
    generation = canvas_ssh.canvas_workspace_generation(thread)

    target = canvas_ssh.resolve_canvas_workspace_target(thread, generation)

    assert generation == canvas_ssh.vm_canvas_generation(VM_THREAD_ID, VM_ROOTDISK)
    assert target == RemoteWorkspaceTarget(
        thread_id=VM_THREAD_ID,
        generation=generation,
        host=VM_HOST,
        port=22,
        fingerprint=VM_FINGERPRINT,
        vm_runtime=(VM_PROVISION_GENERATION, "vm-uid", VM_LAUNCHER, VM_ROOTDISK),
    )
    # The pool keys by endpoint and pin only, like a container target.
    assert target.pool_key == (
        VM_THREAD_ID,
        str(generation),
        VM_HOST,
        22,
        VM_FINGERPRINT,
    )


def test_vm_generation_follows_the_disk_not_the_runtime() -> None:
    before = canvas_ssh.canvas_workspace_generation(vm_thread())
    restored = canvas_ssh.canvas_workspace_generation(vm_thread(restored_vm_context()))
    new_disk = canvas_ssh.canvas_workspace_generation(
        vm_thread(
            restored_vm_context(rootdisk_pvc_uid="abababab-abab-4bab-8bab-abababababab")
        )
    )
    other_thread = canvas_ssh.canvas_workspace_generation(
        vm_thread(thread_id="b4444444-4444-4444-8444-444444444444")
    )

    # A restore onto the kept rootdisk is the same file namespace...
    assert restored == before
    # ...but the endpoint is not, so the target (and its proof) differ.
    assert canvas_ssh.resolve_canvas_workspace_target(
        vm_thread(restored_vm_context()), restored
    ) != canvas_ssh.resolve_canvas_workspace_target(vm_thread(), before)
    assert new_disk != before
    assert other_thread != before


def test_container_resolution_is_unchanged_and_never_sees_a_vm() -> None:
    container = _thread()
    assert canvas_ssh.canvas_workspace_generation(container) == GENERATION
    assert canvas_ssh.resolve_canvas_workspace_target(
        container, GENERATION
    ) == canvas_ssh.resolve_remote_workspace_target(container, GENERATION)
    assert (
        canvas_ssh.resolve_canvas_workspace_target(container, GENERATION).vm_runtime
        is None
    )

    # SSH access and first-use callers rely on the binding resolver never
    # resolving a VM; that stays so.
    vm = vm_thread()
    with pytest.raises(CanvasSSHError):
        canvas_ssh.resolve_remote_workspace_target(
            vm, canvas_ssh.canvas_workspace_generation(vm)
        )


def test_vm_target_fails_its_row_check_on_every_lifecycle_change() -> None:
    thread = vm_thread()
    target = canvas_ssh.resolve_canvas_workspace_target(
        thread, canvas_ssh.canvas_workspace_generation(thread)
    )
    canvas_ssh.require_same_remote_workspace(thread, target)

    for changed in (
        vm_thread(vm_context(status="suspending")),
        vm_thread(vm_context(_suspend_remote_io_closed="lease-1")),
        vm_thread(vm_context(active_pod_uid=RESTORED_LAUNCHER)),
        vm_thread(restored_vm_context()),
        vm_thread(vm_context(rootdisk_pvc_uid="abababab-abab-4bab-8bab-abababababab")),
        {"id": VM_THREAD_ID, "metadata": {}},
    ):
        with pytest.raises(CanvasSSHError):
            canvas_ssh.require_same_remote_workspace(changed, target)


def test_container_target_fails_once_a_vm_takes_over() -> None:
    container = _thread()
    target = canvas_ssh.resolve_remote_workspace_target(container, GENERATION)
    canvas_ssh.require_same_remote_workspace(container, target)

    taken_over = _thread()
    taken_over["metadata"]["vm"] = vm_context()
    with pytest.raises(CanvasSSHError) as error:
        canvas_ssh.require_same_remote_workspace(taken_over, target)
    assert error.value.code == "workspace_generation_changed"


@pytest.mark.parametrize(
    ("mode", "reachable", "expected"),
    [
        ("same-cluster", True, True),
        ("same-cluster", False, False),
        ("external", True, False),
        ("off", True, False),
    ],
)
def test_vm_route_is_same_cluster_only(monkeypatch, mode, reachable, expected) -> None:
    monkeypatch.setenv("VM_MODE", mode)

    assert (
        canvas_ssh.vm_canvas_route_available(VM_HOST, can_reach=lambda _h: reachable)
        is expected
    )


def test_tailnet_vm_route_is_refused_without_network_io(monkeypatch) -> None:
    monkeypatch.setenv("VM_MODE", "external")
    monkeypatch.delenv("ORCHESTRATOR_HAS_TAILNET_ROUTE", raising=False)

    def forbidden(host):
        raise AssertionError(host)

    assert (
        canvas_ssh.vm_canvas_route_available("100.64.23.180", can_reach=forbidden)
        is False
    )


def _vm_target(thread: dict | None = None) -> RemoteWorkspaceTarget:
    thread = thread or vm_thread()
    return canvas_ssh.resolve_canvas_workspace_target(
        thread, canvas_ssh.canvas_workspace_generation(thread)
    )


@pytest.mark.asyncio
async def test_vm_attestation_uses_the_provisioner_singleton_at_call_time(
    monkeypatch,
) -> None:
    thread = vm_thread()
    provisioner = FakeVMProvisioner(thread)
    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", provisioner)

    assert await canvas_ssh.attest_vm_workspace_target(_vm_target(thread)) == "current"
    assert provisioner.calls == [(VM_THREAD_ID, "thread")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "overrides",
    [
        {"workspace_generation": RESTORED_PROVISION_GENERATION},
        {"vm_uid": "other-vm"},
        {"launcher_pod_uid": RESTORED_LAUNCHER},
        {"runtime_incarnation": RESTORED_LAUNCHER},
        {"rootdisk_pvc_uid": "abababab-abab-4bab-8bab-abababababab"},
        {"host": RESTORED_HOST},
        {"port": 2222},
        {"ssh_host_key_fingerprint": RESTORED_FINGERPRINT},
    ],
)
async def test_vm_attestation_of_another_runtime_is_changed(
    monkeypatch, overrides
) -> None:
    thread = vm_thread()
    provisioner = FakeVMProvisioner(thread)
    provisioner.proof_overrides = overrides
    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", provisioner)

    assert await canvas_ssh.attest_vm_workspace_target(_vm_target(thread)) == "changed"


class _RecoveryRequired(Exception):
    def __init__(self, code: WorkspaceRecoveryCode) -> None:
        super().__init__(code.value)
        self.recovery_code = code


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (_RecoveryRequired(WorkspaceRecoveryCode.REPLACEMENT_OBSERVED), "changed"),
        (_RecoveryRequired(WorkspaceRecoveryCode.RUNTIME_NOT_READY), "changed"),
        (_RecoveryRequired(WorkspaceRecoveryCode.TRANSPORT_UNAVAILABLE), "unknown"),
        (RuntimeError("VM controller status is unauthenticated"), "unknown"),
        (OSError("controller down"), "unknown"),
    ],
)
async def test_vm_attestation_failures_are_never_positive(
    monkeypatch, error, expected
) -> None:
    thread = vm_thread()
    provisioner = FakeVMProvisioner(thread)
    provisioner.error = error
    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", provisioner)

    assert await canvas_ssh.attest_vm_workspace_target(_vm_target(thread)) == expected


@pytest.mark.asyncio
async def test_vm_attestation_without_disk_proof_or_attester_is_unknown(
    monkeypatch,
) -> None:
    thread = vm_thread()
    provisioner = FakeVMProvisioner(thread)
    provisioner.proof_overrides = {"rootdisk_pvc_uid": None}
    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", provisioner)
    assert await canvas_ssh.attest_vm_workspace_target(_vm_target(thread)) == "unknown"

    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", object())
    assert await canvas_ssh.attest_vm_workspace_target(_vm_target(thread)) == "unknown"


@pytest.mark.asyncio
async def test_vm_attestation_is_bounded(monkeypatch) -> None:
    class Hanging:
        async def attest_workspace_runtime(self, owner_id, *, entity_type):
            del owner_id, entity_type
            await asyncio.sleep(3600)

    monkeypatch.setattr(vm_provisioner_module, "vm_provisioner", Hanging())
    monkeypatch.setattr(canvas_ssh, "VM_ATTESTATION_TIMEOUT_SECONDS", 0.01)

    assert await canvas_ssh.attest_vm_workspace_target(_vm_target()) == "unknown"


@pytest.mark.asyncio
async def test_a_container_target_has_no_controller_proof() -> None:
    with pytest.raises(ValueError):
        await canvas_ssh.attest_vm_workspace_target(_target())


def test_proof_match_requires_the_exact_runtime() -> None:
    target = _vm_target()
    assert canvas_ssh.vm_runtime_proof_matches(target, proof_for(vm_context()))
    assert not canvas_ssh.vm_runtime_proof_matches(target, None)
    assert not canvas_ssh.vm_runtime_proof_matches(_target(), proof_for(vm_context()))
    assert not canvas_ssh.vm_runtime_proof_matches(
        target, proof_for(vm_context(), port="not-a-port")
    )
