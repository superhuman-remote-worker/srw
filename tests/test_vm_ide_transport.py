"""The VM IDE talks to guest loopback through a pinned SSH channel."""

from contextlib import asynccontextmanager
import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import websockets


def _proof():
    from orchestrator.services.container_provisioner import WorkspaceRuntimeAttestation

    return WorkspaceRuntimeAttestation(
        backing_id="k8s-vmi:" + str(uuid4()),
        workspace_generation=str(uuid4()),
        runtime_incarnation=str(uuid4()),
        ssh_host_key_fingerprint="SHA256:" + "a" * 43,
        host="10.42.0.7", pod_ip="10.42.0.7", port=22,
        vm_uid=str(uuid4()), launcher_pod_uid=str(uuid4()),
        vmi_uid=str(uuid4()), rootdisk_pvc_uid=str(uuid4()),
    )


class _Writer:
    def __init__(self):
        self.data = b""
        self.closed = False

    def write(self, data):
        self.data += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


class _Reader:
    def __init__(self, payload):
        self.payload = payload

    async def read(self, amount):
        value, self.payload = self.payload[:amount], self.payload[amount:]
        return value


class _Pool:
    def __init__(self, connection):
        self.connection = connection
        self.targets = []

    @asynccontextmanager
    async def checkout(self, *, target, key_path):
        self.targets.append((target, key_path))
        yield self.connection


@pytest.mark.asyncio
async def test_start_probes_guest_loopback_only_after_pinned_current_runtime():
    from orchestrator.services.vm_ide_transport import VMIDETransport

    proof = _proof()
    writer = _Writer()
    connection = SimpleNamespace(
        run=AsyncMock(return_value=SimpleNamespace(exit_status=0)),
        open_connection=AsyncMock(return_value=(
            _Reader(b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\n\r\n"
                    b'{"status":"alive"}'), writer,
        )),
    )
    provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock(return_value=proof))
    pool = _Pool(connection)
    transport = VMIDETransport(provisioner, pool=pool, key_path="/private/guest-key")
    assert await transport.start_and_probe(
        "job-1", owner_kind="job",
        expected_generation=proof.workspace_generation, expected_vm_uid=proof.vm_uid,
    ) == proof
    connection.run.assert_awaited_once_with(
        "systemctl --user start srw-code-server-user.service", check=False,
    )
    connection.open_connection.assert_awaited_once_with("127.0.0.1", 8080)
    assert b"GET /healthz HTTP/1.1" in writer.data
    assert writer.closed
    assert pool.targets[0][0].fingerprint == proof.ssh_host_key_fingerprint

    connection.open_connection.reset_mock()
    connection.open_connection.return_value = (
        _Reader(b"HTTP/1.1 200 OK\r\nContent-Length: 18\r\n\r\n"
                b'{"status":"alive"}'), _Writer(),
    )
    connection.run.reset_mock()
    assert await transport.probe(
        "job-1", owner_kind="job",
        expected_generation=proof.workspace_generation, expected_vm_uid=proof.vm_uid,
    ) == proof
    connection.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_changed_attestation_and_missing_older_image_unit_refuse():
    from orchestrator.services.vm_ide_transport import VMIDETransport, VMIDEUnavailable

    proof = _proof()
    connection = SimpleNamespace(
        run=AsyncMock(return_value=SimpleNamespace(exit_status=1)),
        open_connection=AsyncMock(),
    )
    provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock(return_value=proof))
    transport = VMIDETransport(provisioner, pool=_Pool(connection), key_path="/private/key")
    with pytest.raises(VMIDEUnavailable, match="ide_guest_unit_unavailable"):
        await transport.start_and_probe(
            "job-1", owner_kind="job",
            expected_generation=proof.workspace_generation, expected_vm_uid=proof.vm_uid,
        )
    connection.open_connection.assert_not_awaited()

    provisioner.attest_workspace_runtime = AsyncMock(
        side_effect=[proof, replace(proof, vm_uid=str(uuid4()))],
    )
    connection.run.reset_mock()
    with pytest.raises(VMIDEUnavailable, match="ide_runtime_changed"):
        await transport.start_and_probe(
            "job-1", owner_kind="job",
            expected_generation=proof.workspace_generation, expected_vm_uid=proof.vm_uid,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["job", "thread"])
@pytest.mark.parametrize("changed", ["vm_uid", "workspace_generation"])
async def test_start_never_runs_guest_unit_for_a_successor_of_the_admitted_lease(
    owner_kind, changed,
):
    from orchestrator.services.vm_ide_transport import VMIDETransport, VMIDEUnavailable

    admitted = _proof()
    successor = replace(admitted, **{changed: str(uuid4())})
    connection = SimpleNamespace(run=AsyncMock(), open_connection=AsyncMock())
    provisioner = SimpleNamespace(
        attest_workspace_runtime=AsyncMock(return_value=successor)
    )
    transport = VMIDETransport(provisioner, pool=_Pool(connection), key_path="/private/key")
    with pytest.raises(VMIDEUnavailable, match="ide_runtime_changed"):
        await transport.start_and_probe(
            "owner-1", owner_kind=owner_kind,
            expected_generation=admitted.workspace_generation,
            expected_vm_uid=admitted.vm_uid,
        )
    connection.run.assert_not_awaited()
    connection.open_connection.assert_not_awaited()
    with pytest.raises(VMIDEUnavailable, match="ide_runtime_changed"):
        await transport.probe(
            "owner-1", owner_kind=owner_kind,
            expected_generation=admitted.workspace_generation,
            expected_vm_uid=admitted.vm_uid,
        )
    connection.run.assert_not_awaited()


@pytest.mark.asyncio
async def test_http_bytes_only_enter_exact_guest_loopback_after_recheck():
    from orchestrator.services.ide_proxy import IdeProxyTarget
    from orchestrator.services.vm_ide_transport import VMIDETransport, VMIDEUnavailable

    proof = _proof()
    target = IdeProxyTarget(
        entity_id="job-1", owner_kind="job", backend="vm", scope="vm",
        host=proof.host, port=proof.port,
        identity=(proof.workspace_generation, proof.vm_uid, proof.vmi_uid,
                  proof.launcher_pod_uid, proof.rootdisk_pvc_uid,
                  proof.ssh_host_key_fingerprint),
    )
    writer = _Writer()
    connection = SimpleNamespace(open_connection=AsyncMock(return_value=(
        _Reader(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n"
                b"Content-Length: 4\r\n\r\nbody"), writer,
    )))
    provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock(return_value=proof))
    transport = VMIDETransport(provisioner, pool=_Pool(connection), key_path="/private/key")
    response = await transport.request_http(
        target, method="GET", url="http://127.0.0.1:8080/workspace?folder=src",
        headers={"accept": "text/plain", "host": "untrusted.example"},
    )
    assert response.status_code == 200 and response.body == b"body"
    assert b"GET /workspace?folder=src HTTP/1.1" in writer.data
    assert b"host: 127.0.0.1:8080" in writer.data
    assert b"untrusted.example" not in writer.data

    provisioner.attest_workspace_runtime = AsyncMock(
        side_effect=[proof, replace(proof, launcher_pod_uid=str(uuid4()))],
    )
    writer = _Writer()
    connection.open_connection.return_value = (_Reader(b""), writer)
    with pytest.raises(VMIDEUnavailable, match="ide_runtime_changed"):
        await transport.request_http(target, method="GET",
                                     url="http://127.0.0.1:8080/",
                                     headers={})
    assert writer.data == b""


@pytest.mark.asyncio
async def test_direct_channel_websocket_upgrades_and_relays_frames():
    from orchestrator.services.ide_proxy import IdeProxyTarget
    from orchestrator.services.vm_ide_transport import VMIDETransport

    async def echo(socket):
        async for message in socket:
            await socket.send(message)

    proof = _proof()
    target = IdeProxyTarget(
        entity_id="job-1", owner_kind="job", backend="vm", scope="vm",
        host=proof.host, port=proof.port,
        identity=(proof.workspace_generation, proof.vm_uid, proof.vmi_uid,
                  proof.launcher_pod_uid, proof.rootdisk_pvc_uid,
                  proof.ssh_host_key_fingerprint),
    )
    async with websockets.serve(echo, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]

        async def open_channel(host, destination):
            assert host == "127.0.0.1" and destination == 8080
            return await asyncio.open_connection("127.0.0.1", port)

        connection = SimpleNamespace(open_connection=open_channel)
        provisioner = SimpleNamespace(attest_workspace_runtime=AsyncMock(return_value=proof))
        transport = VMIDETransport(provisioner, pool=_Pool(connection), key_path="/private/key")
        async with transport.open_websocket(target, path="/vscode") as channel:
            await channel.send("hello")
            assert await asyncio.wait_for(channel.recv(), timeout=5) == "hello"
            await channel.send(b"\x00\x01")
            assert await asyncio.wait_for(channel.recv(), timeout=5) == b"\x00\x01"
