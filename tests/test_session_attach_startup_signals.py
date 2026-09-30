"""Startup termination belongs to attach cancellation and retains cleanup proof."""

import asyncio
import signal
from unittest.mock import Mock

import pytest

from agent.api import persistent_app as pa


@pytest.mark.asyncio
async def test_startup_sigterm_cancels_owned_attach_and_chains_server_handler(
    monkeypatch,
):
    handlers = {signal.SIGTERM: Mock(), signal.SIGINT: Mock()}
    prior = dict(handlers)
    monkeypatch.setattr(signal, "getsignal", lambda signum: handlers[signum])
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.__setitem__(signum, handler)
    )
    entered, cleaned = asyncio.Event(), asyncio.Event()

    async def attach(thread_id):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            cleaned.set()

    monkeypatch.setattr(pa._session_attach, "attach", attach)
    fence = Mock()
    task = asyncio.create_task(
        pa._session_attach.attach_during_startup(
            "captured", on_shutdown=fence, cleanup_timeout=0.1
        )
    )
    await entered.wait()
    handlers[signal.SIGTERM](signal.SIGTERM, None)
    assert await asyncio.wait_for(task, 1) is False
    assert cleaned.is_set()
    fence.assert_called_once()
    prior[signal.SIGTERM].assert_called_once_with(signal.SIGTERM, None)
    assert handlers == prior


@pytest.mark.asyncio
async def test_startup_cancellation_timeout_is_bounded_and_never_claims_quiescence(
    monkeypatch,
):
    handlers = {signal.SIGTERM: Mock(), signal.SIGINT: Mock()}
    monkeypatch.setattr(signal, "getsignal", lambda signum: handlers[signum])
    monkeypatch.setattr(
        signal, "signal", lambda signum, handler: handlers.__setitem__(signum, handler)
    )
    entered, finish = asyncio.Event(), asyncio.Event()

    async def attach(thread_id):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            await finish.wait()  # an unproven remote cleanup remains owned
            raise

    monkeypatch.setattr(pa._session_attach, "attach", attach)
    task = asyncio.create_task(
        pa._session_attach.attach_during_startup(
            "captured", on_shutdown=Mock(), cleanup_timeout=0.01
        )
    )
    await entered.wait()
    handlers[signal.SIGTERM](signal.SIGTERM, None)
    try:
        assert await asyncio.wait_for(task, 1) is False
        assert pa._session_attach.startup_task is not None
        assert not pa._session_attach.startup_task.done()
        assert pa._session_attach.release_receipt is None
    finally:
        finish.set()
        await asyncio.gather(pa._session_attach.startup_task, return_exceptions=True)
