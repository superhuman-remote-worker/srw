"""Real child processes must stop before an IDE key-operation owner exits."""

import asyncio
import logging
import re
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from orchestrator.services.ide_session import IdeSessionService
from shared.runtime.core.managed_repository import (
    managed_repository_agent_launch_command,
    managed_repository_agent_zero_command,
)


@pytest.mark.parametrize("loop_backend", ["asyncio", "uvloop"])
def test_secret_stdin_reaches_child_before_buffer_erasure(loop_backend):
    loop_factory = (
        asyncio.new_event_loop
        if loop_backend == "asyncio"
        else pytest.importorskip("uvloop").new_event_loop
    )
    secret = bytearray(b"disposable-stdin-canary\n")
    command = [
        sys.executable,
        "-c",
        "import sys; raise SystemExit(0 if sys.stdin.buffer.read() == "
        "b'disposable-stdin-canary\\n' else 65)",
    ]
    with asyncio.Runner(loop_factory=loop_factory) as runner:
        received = runner.run(
            IdeSessionService._run_secret_stdin_process(command, secret)
        )
    assert secret == bytearray(len(secret))
    assert received, "mutable stdin buffer was erased before the child received it"


@pytest.mark.asyncio
async def test_failed_secret_process_reports_only_fixed_phase_and_exit_code(caplog):
    secret_text = "stdin-private-key-canary"
    command_text = (
        "import sys; data=sys.stdin.buffer.read(); "
        "sys.stdout.buffer.write(data+b' stdout-canary'); "
        "sys.stderr.buffer.write(data+b' stderr-canary'); "
        "raise SystemExit(86)"
    )
    command = [sys.executable, "-c", command_text, "argv-canary"]
    secret = bytearray(secret_text.encode())
    job_id = "11111111-1111-4111-8111-111111111111"

    with caplog.at_level(logging.WARNING, logger="orchestrator.services.ide_session"):
        assert not await IdeSessionService._run_secret_stdin_process(
            command, secret, diagnostic_job_id=job_id
        )

    diagnostic = "\n".join(record.getMessage() for record in caplog.records)
    assert f"job={job_id}" in diagnostic
    assert "phase=child_exit" in diagnostic
    assert "exit_code=86" in diagnostic
    for forbidden in (
        secret_text,
        "stdout-canary",
        "stderr-canary",
        "argv-canary",
        command_text,
    ):
        assert forbidden not in diagnostic
    assert secret == bytearray(len(secret))


@pytest.mark.asyncio
async def test_generated_agent_failure_reports_code_without_exposing_command(caplog):
    # Exercise the real generated shell rather than a fake child return code.
    with tempfile.TemporaryDirectory(prefix="srw-git-diag-", dir="/tmp") as home:
        authority_id = str(uuid4())
        command = managed_repository_agent_launch_command(
            home_path=home, authority_id=authority_id, generation=1
        )
        secret = bytearray(b"invalid-private-key-canary")
        with caplog.at_level(
            logging.WARNING, logger="orchestrator.services.ide_session"
        ):
            assert not await IdeSessionService._run_secret_stdin_process(
                ["bash", "-c", command],
                secret,
                diagnostic_job_id="22222222-2222-4222-8222-222222222222",
            )
        diagnostic = "\n".join(record.getMessage() for record in caplog.records)
        assert "phase=child_exit" in diagnostic
        assert re.search(r"exit_code=\d+", diagnostic)
        assert "invalid-private-key-canary" not in diagnostic
        assert home not in diagnostic
        assert command not in diagnostic
        assert secret == bytearray(len(secret))
        zero = await asyncio.create_subprocess_exec(
            "bash",
            "-c",
            managed_repository_agent_zero_command(home_path=home),
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        assert await zero.wait() == 0
        assert not list(Path(home, ".ssh", "srw-managed", "agents").glob("*.state"))


@pytest.mark.asyncio
async def test_secret_stdin_write_failure_is_not_misreported_as_spawn(
    monkeypatch, caplog
):
    class BrokenStdin:
        def write(self, _secret):
            raise OSError("write-error-secret-canary")

    class ExitedChild:
        stdin = BrokenStdin()
        returncode = 41

    async def spawn(*_args, **_kwargs):
        return ExitedChild()

    monkeypatch.setattr(
        "orchestrator.services.ide_session.create_owned_subprocess_exec", spawn
    )
    secret = bytearray(b"stdin-secret-canary")
    with caplog.at_level(logging.WARNING, logger="orchestrator.services.ide_session"):
        assert not await IdeSessionService._run_secret_stdin_process(
            ["secret-command-canary"], secret
        )

    diagnostic = "\n".join(record.getMessage() for record in caplog.records)
    assert "phase=child_io_error" in diagnostic
    assert "phase=child_spawn_error" not in diagnostic
    for forbidden in (
        "write-error-secret-canary",
        "stdin-secret-canary",
        "secret-command-canary",
    ):
        assert forbidden not in diagnostic
    assert secret == bytearray(len(secret))


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["spawn", "drain", "close", "wait"])
async def test_secret_process_cancellation_joins_child_and_erases_key(
    monkeypatch, tmp_path, phase
):
    real_spawn = asyncio.create_subprocess_exec
    reached = asyncio.Event()
    return_spawn = asyncio.Event()
    terminating = asyncio.Event()
    children = []
    ready = tmp_path / "ready"
    # In the wait case, exercise repeated cancellation while TERM grace is
    # running, then prove the real SIGKILL result was reaped.
    command = (
        "import pathlib, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "sys.stdin.buffer.read(); "
        "pathlib.Path(sys.argv[1]).touch(); time.sleep(60)"
        if phase == "wait"
        else "import time; time.sleep(60)"
    )

    async def spawn(*args, **kwargs):
        child = await real_spawn(*args, **kwargs)
        children.append(child)
        terminate = child.terminate

        def observed_terminate():
            terminate()
            terminating.set()

        monkeypatch.setattr(child, "terminate", observed_terminate)
        if phase == "spawn":
            reached.set()
            await return_spawn.wait()
        elif phase == "drain":
            drain = child.stdin.drain

            async def observed_drain():
                reached.set()
                await drain()

            monkeypatch.setattr(child.stdin, "drain", observed_drain)
        elif phase == "close":

            async def delayed_close():
                reached.set()
                await asyncio.Event().wait()

            monkeypatch.setattr(child.stdin, "wait_closed", delayed_close)
        return child

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    # More than the pipe and transport buffers so the drain case really waits.
    secret = bytearray(b"test-key" * (262_144 if phase == "drain" else 1))
    task = asyncio.create_task(
        IdeSessionService._run_secret_stdin_process(
            [sys.executable, "-c", command, str(ready)], secret
        )
    )
    completed_promptly = False
    try:
        async with asyncio.timeout(5):
            if phase == "wait":
                while not ready.exists():
                    await asyncio.sleep(0.01)
            else:
                await reached.wait()
        task.cancel()
        return_spawn.set()
        if phase == "wait":
            # With the old full-operation shield, no TERM is sent. Bound the
            # observation and let finally reap the child even on that failure.
            await asyncio.wait_for(terminating.wait(), timeout=2)
            task.cancel()
        done, _ = await asyncio.wait({task}, timeout=3)
        completed_promptly = task in done
    finally:
        return_spawn.set()
        for child in children:
            if child.returncode is None:
                child.kill()
            await child.wait()
        if not task.done():
            task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert completed_promptly, "lease cancellation waited for the command timeout"
    assert len(children) == 1
    assert children[0].returncode is not None
    if phase == "wait":
        assert children[0].returncode == -9
    assert secret == bytearray(len(secret))


@pytest.mark.asyncio
async def test_secret_process_timeout_reaps_real_child_and_erases_key(monkeypatch):
    real_spawn = asyncio.create_subprocess_exec
    children = []

    async def spawn(*args, **kwargs):
        child = await real_spawn(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    secret = bytearray(b"test-key")
    assert not await IdeSessionService._run_secret_stdin_process(
        [sys.executable, "-c", "import time; time.sleep(60)"], secret, timeout=0.05
    )
    assert len(children) == 1
    assert children[0].returncode is not None
    assert secret == bytearray(len(secret))
