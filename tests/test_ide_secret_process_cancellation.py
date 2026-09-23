"""Real child processes must stop before an IDE key-operation owner exits."""

import asyncio
import sys

import pytest

from orchestrator.services.ide_session import IdeSessionService


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["spawn", "drain", "wait"])
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
