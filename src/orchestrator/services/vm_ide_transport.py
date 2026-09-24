"""Attested guest-loopback IDE startup through host-key-pinned SSH.

No guest coordinate is an IDE address.  The controller first proves the exact
VM/VMI/launcher/PVC and SSH host key; the SSH handshake pins that key; a
direct-tcpip channel reaches only the guest's loopback code-server port.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID
from urllib.parse import urlsplit

import h11
from websockets.client import ClientProtocol
from websockets.frames import Frame, OP_BINARY, OP_CONT, OP_TEXT
from websockets.http11 import Response as WSResponse
from websockets.protocol import State as WSState
from websockets.uri import parse_uri

from orchestrator.services.canvas_ssh import (
    CANVAS_LOOPBACK_HOST,
    PINNED_SSH_TRANSPORT_POOL,
    RemoteWorkspaceTarget,
)

IDE_LOOPBACK_PORT = 8080
IDE_START_COMMAND = "systemctl --user start srw-code-server-user.service"


class VMIDEUnavailable(RuntimeError):
    """Safe public code for one unavailable exact VM IDE endpoint."""

    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


def matches_admitted_runtime(proof: Any, generation: Any, vm_uid: Any) -> bool:
    """Compare the guest proof to the exact durable access lease identity."""
    try:
        return (
            UUID(str(proof.workspace_generation)) == UUID(str(generation))
            and UUID(str(proof.vm_uid)) == UUID(str(vm_uid))
        )
    except (TypeError, ValueError, AttributeError):
        return False


@dataclass(frozen=True)
class VMIDEHTTPResponse:
    status_code: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


class VMIDEWebSocket:
    """Small SansIO WebSocket adapter over one authenticated SSH channel."""

    def __init__(self, reader: Any, writer: Any, protocol: ClientProtocol) -> None:
        self.reader = reader
        self.writer = writer
        self.protocol = protocol
        self._fragment = bytearray()
        self._fragment_type: int | None = None

    async def _flush(self) -> None:
        for packet in self.protocol.data_to_send():
            self.writer.write(packet)
        await self.writer.drain()

    async def send(self, value: str | bytes) -> None:
        if isinstance(value, str):
            self.protocol.send_text(value.encode("utf-8"))
        else:
            self.protocol.send_binary(value)
        await self._flush()

    async def recv(self) -> str | bytes:
        while True:
            events = self.protocol.events_received()
            for event in events:
                if not isinstance(event, Frame):
                    continue
                if event.opcode in {OP_TEXT, OP_BINARY}:
                    self._fragment_type = event.opcode
                    self._fragment = bytearray(event.data)
                elif event.opcode == OP_CONT and self._fragment_type is not None:
                    self._fragment.extend(event.data)
                else:
                    continue
                if len(self._fragment) > 16 * 1024 * 1024:
                    raise VMIDEUnavailable("ide_ws_too_large")
                if event.fin:
                    data = bytes(self._fragment)
                    kind = self._fragment_type
                    self._fragment.clear()
                    self._fragment_type = None
                    return data.decode("utf-8") if kind == OP_TEXT else data
            if self.protocol.state is WSState.CLOSED:
                raise StopAsyncIteration
            data = await self.reader.read(65536)
            self.protocol.receive_data(data)
            await self._flush()
            if not data:
                raise StopAsyncIteration

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.recv()

    async def close(self) -> None:
        if self.protocol.state is WSState.OPEN:
            self.protocol.send_close(1000)
            await self._flush()


def _target(owner_id: str, proof: Any) -> RemoteWorkspaceTarget:
    try:
        if not all(
            (
                proof.vm_uid,
                proof.vmi_uid,
                proof.launcher_pod_uid,
                proof.rootdisk_pvc_uid,
                proof.ssh_host_key_fingerprint,
            )
        ):
            raise ValueError
        generation = UUID(proof.workspace_generation)
        if not proof.ssh_host_key_fingerprint.startswith("SHA256:"):
            raise ValueError
    except (AttributeError, TypeError, ValueError) as exc:
        raise VMIDEUnavailable("ide_runtime_unproven") from exc
    return RemoteWorkspaceTarget(
        thread_id=owner_id,
        generation=generation,
        host=proof.host,
        port=proof.port,
        fingerprint=proof.ssh_host_key_fingerprint,
    )


async def _probe_code_server(connection: Any) -> bool:
    reader, writer = await connection.open_connection(
        CANVAS_LOOPBACK_HOST,
        IDE_LOOPBACK_PORT,
    )
    try:
        client = h11.Connection(h11.CLIENT)
        writer.write(
            client.send(
                h11.Request(
                    method=b"GET",
                    target=b"/healthz",
                    headers=[
                        (b"host", b"127.0.0.1:8080"),
                        (b"accept", b"application/json"),
                        (b"connection", b"close"),
                    ],
                )
            )
        )
        writer.write(client.send(h11.EndOfMessage()))
        await writer.drain()
        status = None
        body = bytearray()
        async with asyncio.timeout(5):
            while True:
                event = client.next_event()
                if event is h11.NEED_DATA:
                    data = await reader.read(4096)
                    client.receive_data(data)
                    if not data and client.next_event() is h11.NEED_DATA:
                        return False
                    continue
                if isinstance(event, h11.Response):
                    status = event.status_code
                elif isinstance(event, h11.Data):
                    body.extend(event.data)
                    if len(body) > 4096:
                        return False
                elif isinstance(event, h11.EndOfMessage):
                    break
                elif event is h11.PAUSED:
                    return False
        # code-server's local health endpoint has a precise small response.
        return status == 200 and b'"status":"alive"' in body.replace(b" ", b"")
    finally:
        writer.close()
        await writer.wait_closed()


class VMIDETransport:
    def __init__(
        self, provisioner: Any, *, pool: Any = None, key_path: str | None = None
    ) -> None:
        self.provisioner = provisioner
        self.pool = pool or PINNED_SSH_TRANSPORT_POOL
        self.key_path = key_path or os.environ.get("SSH_KEY_PATH", "")

    async def _attest(self, owner_id: str, owner_kind: str) -> Any:
        try:
            proof = await self.provisioner.attest_workspace_runtime(
                owner_id,
                entity_type=owner_kind,
            )
            _target(owner_id, proof)
        except Exception as exc:
            raise VMIDEUnavailable("ide_runtime_unproven") from exc
        return proof

    @staticmethod
    def _target_matches(proof: Any, target: Any) -> bool:
        return (
            target.backend == "vm"
            and target.host == proof.host
            and target.port == proof.port
            and target.identity
            == (
                proof.workspace_generation,
                proof.vm_uid,
                proof.vmi_uid,
                proof.launcher_pod_uid,
                proof.rootdisk_pvc_uid,
                proof.ssh_host_key_fingerprint,
            )
        )

    async def request_http(
        self,
        target: Any,
        *,
        method: str,
        url: str,
        headers: dict[str, str],
        content: bytes | None = None,
        max_response_body_bytes: int = 32 * 1024 * 1024,
    ) -> VMIDEHTTPResponse:
        """One unpooled HTTP exchange over exact SSH direct-tcpip."""
        if not self.key_path:
            raise VMIDEUnavailable("ide_guest_key_unavailable")
        if method not in {"GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"}:
            raise VMIDEUnavailable("ide_method_unavailable")
        parsed = urlsplit(url)
        path = parsed.path + (("?" + parsed.query) if parsed.query else "")
        if not path.startswith("/") or len(path) > 8192:
            raise VMIDEUnavailable("ide_request_invalid")
        if content is not None and len(content) > 16 * 1024 * 1024:
            raise VMIDEUnavailable("ide_request_too_large")
        initial = await self._attest(target.entity_id, target.owner_kind)
        if not self._target_matches(initial, target):
            raise VMIDEUnavailable("ide_runtime_changed")
        try:
            async with self.pool.checkout(
                target=_target(target.entity_id, initial),
                key_path=self.key_path,
            ) as connection:
                reader, writer = await connection.open_connection(
                    CANVAS_LOOPBACK_HOST,
                    IDE_LOOPBACK_PORT,
                )
                try:
                    if not self._target_matches(
                        await self._attest(target.entity_id, target.owner_kind),
                        target,
                    ):
                        raise VMIDEUnavailable("ide_runtime_changed")
                    client = h11.Connection(h11.CLIENT)
                    forwarded = [
                        (key.lower().encode("ascii"), value.encode("latin-1"))
                        for key, value in headers.items()
                        if key.lower() != "host"
                    ]
                    forwarded.append((b"host", b"127.0.0.1:8080"))
                    forwarded.append((b"connection", b"close"))
                    if content is not None:
                        forwarded.append(
                            (b"content-length", str(len(content)).encode())
                        )
                    writer.write(
                        client.send(
                            h11.Request(
                                method=method.encode("ascii"),
                                target=path.encode("ascii"),
                                headers=forwarded,
                            )
                        )
                    )
                    if content:
                        writer.write(client.send(h11.Data(data=content)))
                    writer.write(client.send(h11.EndOfMessage()))
                    await writer.drain()
                    status = None
                    response_headers: tuple[tuple[str, str], ...] = ()
                    body = bytearray()
                    async with asyncio.timeout(300):
                        while True:
                            event = client.next_event()
                            if event is h11.NEED_DATA:
                                data = await reader.read(65536)
                                client.receive_data(data)
                                if not data and client.next_event() is h11.NEED_DATA:
                                    raise VMIDEUnavailable("ide_response_incomplete")
                                continue
                            if isinstance(event, h11.Response):
                                status = event.status_code
                                response_headers = tuple(
                                    (key.decode("ascii"), value.decode("latin-1"))
                                    for key, value in event.headers
                                )
                            elif isinstance(event, h11.Data):
                                body.extend(event.data)
                                if len(body) > max_response_body_bytes:
                                    raise VMIDEUnavailable("ide_response_too_large")
                            elif isinstance(event, h11.EndOfMessage):
                                break
                            elif event is h11.PAUSED:
                                raise VMIDEUnavailable("ide_response_invalid")
                    if status is None or not self._target_matches(
                        await self._attest(target.entity_id, target.owner_kind),
                        target,
                    ):
                        raise VMIDEUnavailable("ide_runtime_changed")
                    return VMIDEHTTPResponse(status, response_headers, bytes(body))
                finally:
                    writer.close()
                    await writer.wait_closed()
        except VMIDEUnavailable:
            raise
        except Exception as exc:
            raise VMIDEUnavailable("ide_guest_transport_unavailable") from exc

    @asynccontextmanager
    async def open_websocket(self, target: Any, *, path: str):
        """Upgrade a guest-loopback direct channel; no local TCP listener."""
        if not self.key_path or not path.startswith("/") or len(path) > 8192:
            raise VMIDEUnavailable("ide_ws_request_invalid")
        initial = await self._attest(target.entity_id, target.owner_kind)
        if not self._target_matches(initial, target):
            raise VMIDEUnavailable("ide_runtime_changed")
        try:
            async with self.pool.checkout(
                target=_target(target.entity_id, initial),
                key_path=self.key_path,
            ) as connection:
                reader, writer = await connection.open_connection(
                    CANVAS_LOOPBACK_HOST,
                    IDE_LOOPBACK_PORT,
                )
                try:
                    if not self._target_matches(
                        await self._attest(target.entity_id, target.owner_kind),
                        target,
                    ):
                        raise VMIDEUnavailable("ide_runtime_changed")
                    protocol = ClientProtocol(
                        parse_uri("ws://127.0.0.1:8080" + path),
                        max_size=16 * 1024 * 1024,
                    )
                    request = protocol.connect()
                    protocol.send_request(request)
                    for packet in protocol.data_to_send():
                        writer.write(packet)
                    await writer.drain()
                    async with asyncio.timeout(10):
                        while protocol.state is WSState.CONNECTING:
                            data = await reader.read(65536)
                            if not data:
                                raise VMIDEUnavailable("ide_ws_handshake_failed")
                            protocol.receive_data(data)
                    if not any(
                        isinstance(event, WSResponse) and event.status_code == 101
                        for event in protocol.events_received()
                    ) or not self._target_matches(
                        await self._attest(target.entity_id, target.owner_kind),
                        target,
                    ):
                        raise VMIDEUnavailable("ide_ws_handshake_failed")
                    channel = VMIDEWebSocket(reader, writer, protocol)
                    try:
                        yield channel
                    finally:
                        await channel.close()
                finally:
                    writer.close()
                    await writer.wait_closed()
        except VMIDEUnavailable:
            raise
        except Exception as exc:
            raise VMIDEUnavailable("ide_guest_transport_unavailable") from exc

    async def start_and_probe(
        self, owner_id: str, *, owner_kind: str,
        expected_generation: Any, expected_vm_uid: Any,
    ) -> Any:
        """Start a fixed dormant user unit, then prove its loopback service."""
        if not self.key_path:
            raise VMIDEUnavailable("ide_guest_key_unavailable")
        initial = await self._attest(owner_id, owner_kind)
        if not matches_admitted_runtime(initial, expected_generation, expected_vm_uid):
            raise VMIDEUnavailable("ide_runtime_changed")
        try:
            async with self.pool.checkout(
                target=_target(owner_id, initial),
                key_path=self.key_path,
            ) as connection:
                if await self._attest(owner_id, owner_kind) != initial:
                    raise VMIDEUnavailable("ide_runtime_changed")
                result = await connection.run(IDE_START_COMMAND, check=False)
                if result.exit_status != 0:
                    # Older immutable images have no user unit.  Never fall
                    # back to sudo, a shell daemon, or an unproven endpoint.
                    raise VMIDEUnavailable("ide_guest_unit_unavailable")
                if not await _probe_code_server(connection):
                    raise VMIDEUnavailable("ide_service_unavailable")
                if await self._attest(owner_id, owner_kind) != initial:
                    raise VMIDEUnavailable("ide_runtime_changed")
        except VMIDEUnavailable:
            raise
        except Exception as exc:
            raise VMIDEUnavailable("ide_guest_transport_unavailable") from exc
        return initial

    async def probe(
        self, owner_id: str, *, owner_kind: str,
        expected_generation: Any, expected_vm_uid: Any,
    ) -> Any:
        """Observe an already-started service; status polls never start it."""
        if not self.key_path:
            raise VMIDEUnavailable("ide_guest_key_unavailable")
        initial = await self._attest(owner_id, owner_kind)
        if not matches_admitted_runtime(initial, expected_generation, expected_vm_uid):
            raise VMIDEUnavailable("ide_runtime_changed")
        try:
            async with self.pool.checkout(
                target=_target(owner_id, initial),
                key_path=self.key_path,
            ) as connection:
                if await self._attest(owner_id, owner_kind) != initial:
                    raise VMIDEUnavailable("ide_runtime_changed")
                if not await _probe_code_server(connection):
                    raise VMIDEUnavailable("ide_service_unavailable")
                if await self._attest(owner_id, owner_kind) != initial:
                    raise VMIDEUnavailable("ide_runtime_changed")
        except VMIDEUnavailable:
            raise
        except Exception as exc:
            raise VMIDEUnavailable("ide_guest_transport_unavailable") from exc
        return initial
