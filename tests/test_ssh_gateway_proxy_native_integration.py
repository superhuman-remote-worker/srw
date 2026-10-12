"""Real SSH channels keep native confirmation failures visible after guest EOF."""

import asyncio
from contextlib import AsyncExitStack

import asyncssh
import pytest
from asyncssh.sftp import MIN_SFTP_VERSION

from orchestrator.services.ssh_gateway_proxy import ProxyProcess, proxy_session


class _LocalGuestServer(asyncssh.SSHServer):
    def begin_auth(self, username):
        return False


@pytest.mark.asyncio
@pytest.mark.parametrize("acknowledged", [True, False])
@pytest.mark.parametrize("command", ["write-sentinel-once", "read-until-eof"])
async def test_short_guest_command_keeps_confirmation_result_visible(
    acknowledged, command
):
    """Guest EOF must not close downstream stderr before a failed notice."""
    guest_finished = asyncio.Event()
    started_commands = []

    async def guest_process(process):
        started_commands.append(process.command)
        if process.command == "read-until-eof":
            process.stdout.write(await process.stdin.read())
        else:
            process.stdout.write(b"guest output\n")
        process.stderr.write(b"guest diagnostic\n")
        process.exit(0)
        guest_finished.set()

    guest_key = asyncssh.generate_private_key("ssh-ed25519")
    gateway_key = asyncssh.generate_private_key("ssh-ed25519")
    async with AsyncExitStack() as stack:
        guest = await stack.enter_async_context(
            await asyncssh.create_server(
                _LocalGuestServer,
                "127.0.0.1",
                0,
                server_host_keys=[guest_key],
                process_factory=guest_process,
                encoding=None,
                line_editor=False,
            )
        )
        upstream = await stack.enter_async_context(
            await asyncssh.connect(
                "127.0.0.1",
                port=guest.get_port(),
                known_hosts=(
                    [asyncssh.import_public_key(guest_key.export_public_key())],
                    [],
                    [],
                ),
                agent_path=None,
                config=(),
                encoding=None,
            )
        )

        class ObservedUpstream:
            process = None

            async def create_process(self, **kwargs):
                self.process = await upstream.create_process(**kwargs)
                return self.process

        observed = ObservedUpstream()

        async def native_notice():
            await asyncio.wait_for(guest_finished.wait(), timeout=2)
            # Require the gateway's real upstream channel to process guest
            # EOF and close before returning the control-plane response.
            await asyncio.wait_for(observed.process.wait_closed(), timeout=2)
            return acknowledged

        async def gateway_process(process):
            await proxy_session(process, observed, on_first_use=native_notice)

        class GatewayServer(_LocalGuestServer):
            def session_requested(self):
                return ProxyProcess(gateway_process, None, MIN_SFTP_VERSION, False)

        gateway = await stack.enter_async_context(
            await asyncssh.create_server(
                GatewayServer,
                "127.0.0.1",
                0,
                server_host_keys=[gateway_key],
                encoding=None,
                line_editor=False,
            )
        )
        client = await stack.enter_async_context(
            await asyncssh.connect(
                "127.0.0.1",
                port=gateway.get_port(),
                known_hosts=(
                    [asyncssh.import_public_key(gateway_key.export_public_key())],
                    [],
                    [],
                ),
                agent_path=None,
                config=(),
                encoding=None,
            )
        )
        async with asyncio.timeout(5):
            result = await client.run(
                command,
                input=b"input reached guest\n" if command == "read-until-eof" else None,
            )

    assert started_commands == [command]
    assert result.stdout == (
        b"input reached guest\n" if command == "read-until-eof" else b"guest output\n"
    )
    assert result.stderr.startswith(b"guest diagnostic\n")
    if acknowledged:
        assert result.exit_status == 0
        assert result.stderr == b"guest diagnostic\n"
    else:
        assert result.exit_status == 75
        assert b"command may already have started" in result.stderr
        assert b"Check its outcome before retrying" in result.stderr
