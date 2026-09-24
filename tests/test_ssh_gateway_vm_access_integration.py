"""Real AsyncSSH gateway and VM guest key/host-pin behavior."""

import asyncio
import contextlib
from uuid import uuid4

import asyncssh
import pytest

from orchestrator.services.ssh_gateway_client import SshTarget, TargetUnavailable
from orchestrator.services.ssh_gateway_server import (
    GatewaySSHServer, drain_background_tasks,
)
from tests.test_ssh_gateway_server import _context, _limiter


async def _guest_process(process):
    process.stdout.write(b"guest:" + (process.command or "shell").encode() + b"\n")
    process.exit(0)


@contextlib.asynccontextmanager
async def _guest(tmp_path, client_key):
    host = asyncssh.generate_private_key("ssh-ed25519")
    server = await asyncssh.create_server(
        asyncssh.SSHServer, "127.0.0.1", 0,
        server_host_keys=[host],
        authorized_client_keys=asyncssh.import_authorized_keys(
            client_key.export_public_key().decode(),
        ),
        process_factory=_guest_process,
        sftp_factory=lambda channel: asyncssh.SFTPServer(channel, chroot=str(tmp_path)),
        encoding=None,
    )
    try:
        yield server.sockets[0].getsockname()[1], host.get_fingerprint("sha256")
    finally:
        server.close()
        async with asyncio.timeout(10):
            await server.wait_closed()


@pytest.mark.asyncio
async def test_verified_key_vm_shell_and_sftp_renew_then_close(tmp_path, monkeypatch):
    import orchestrator.services.ssh_gateway_server as gateway_mod

    vm_key = asyncssh.generate_private_key("ssh-ed25519")
    vm_key_path = tmp_path / "vm-key"
    vm_key_path.write_bytes(vm_key.export_private_key())
    monkeypatch.setenv("SSH_KEY_PATH", str(vm_key_path))
    monkeypatch.setattr(gateway_mod, "VM_RENEW_INTERVAL_SECONDS", 0.05)
    (tmp_path / "sentinel.txt").write_text("retained", encoding="utf-8")
    calls = []
    async with _guest(tmp_path, vm_key) as (guest_port, guest_pin):
        target = SshTarget(
            thread_id=str(uuid4()), user_id=str(uuid4()),
            pod_ip="127.0.0.1", pod_port=guest_port,
            host_key_fingerprint=guest_pin, state="live", backend="vm",
            lease_id=str(uuid4()), binding="a" * 64,
        )

        async def resolver(*_):
            raise TargetUnavailable("vm_unsupported")

        async def admit(**kwargs):
            calls.append("admit")
            assert kwargs["connection_id"]
            return target

        async def renew(**kwargs):
            assert kwargs["target"] == target
            calls.append("renew")
            return True

        async def close(**kwargs):
            assert kwargs["target"] == target
            calls.append("close")
            return True

        gateway = await asyncssh.create_server(
            lambda: GatewaySSHServer(_context(
                ca=object(), limiter=_limiter(), resolve=resolver,
                vm_admit=admit, vm_renew=renew, vm_close=close,
            ), "127.0.0.1"),
            "127.0.0.1", 0,
            server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
            encoding=None, line_editor=False,
        )
        try:
            port = gateway.sockets[0].getsockname()[1]
            # No signed authentication, no gateway admission.
            with pytest.raises(asyncssh.PermissionDenied):
                await asyncssh.connect(
                    "127.0.0.1", port=port, username="invalid/handle",
                    client_keys=[asyncssh.generate_private_key("ssh-ed25519")],
                    known_hosts=None,
                    preferred_auth=["publickey"], agent_path=None, config=(),
                )
            assert calls == []
            client = await asyncssh.connect(
                "127.0.0.1", port=port, username="s-7f3a91c2",
                client_keys=[asyncssh.generate_private_key("ssh-ed25519")],
                known_hosts=None, encoding=None,
            )
            try:
                result = await client.run("whoami")
                assert result.stdout == b"guest:whoami\n"
                async with client.start_sftp_client() as sftp:
                    async with sftp.open("sentinel.txt", "rb") as opened:
                        assert await opened.read() == b"retained"
                    await asyncio.sleep(0.12)
                assert "renew" in calls
            finally:
                client.close()
                await client.wait_closed()
            await asyncio.sleep(0.05)
            await drain_background_tasks()
            assert calls[0] == "admit"
            assert calls[-1] == "close"
        finally:
            gateway.close()
            with contextlib.suppress(TimeoutError):
                async with asyncio.timeout(10):
                    await gateway.wait_closed()
