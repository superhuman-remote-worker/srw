"""Loop completion joins the termination owner but is drained only at shutdown."""

import asyncio

import pytest

from agent.api import persistent_app as pa


@pytest.mark.asyncio
async def test_loop_completion_is_tracked_until_its_owned_work_finishes(monkeypatch):
    entered, finished = asyncio.Event(), asyncio.Event()

    async def complete(loop):
        entered.set()
        await finished.wait()

    monkeypatch.setattr(pa._session_termination, "loop_completion_handler", complete)
    loop = asyncio.create_task(asyncio.sleep(0))
    completion = pa._session_termination.start_loop_completion_handler(loop)
    await entered.wait()
    assert completion in pa._session_termination.loop_completion_tasks
    drain = asyncio.create_task(pa._session_termination.drain_loop_completion_tasks())
    await asyncio.sleep(0)
    assert not drain.done()
    finished.set()
    await drain
    await loop
    assert completion.done()
    assert not pa._session_termination.loop_completion_tasks


@pytest.mark.asyncio
async def test_teardown_joins_side_task_finally_before_writer_close_and_identity_reuse():
    import dataclasses
    import logging
    from unittest.mock import AsyncMock, MagicMock
    from agent.api.session_termination import (
        SessionTerminationCoordinator,
        SessionTerminationPorts,
    )

    entered, cancelling, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def side_work():
        try:
            entered.set()
            await asyncio.Future()
        finally:
            cancelling.set()
            await release.wait()

    session = MagicMock()
    session.memory_service = None
    session.workspace_sync = None
    session.workspace_manager = None
    session.shell_owner_token = None
    session.terminal_finalization_attempted = True
    session.quiesce_background_tasks = AsyncMock()
    session.cleanup = AsyncMock(return_value=None)
    writer = MagicMock()
    writer.close = AsyncMock()
    identity = MagicMock(
        thread_id="captured", session_generation=None, attach_token=None
    )
    identity.retirement_identity.return_value = ("captured", None, None)
    values = {f.name: MagicMock() for f in dataclasses.fields(SessionTerminationPorts)}
    values.update(
        session=lambda: session,
        identity=lambda: identity,
        loop_task=lambda: None,
        stateless_mode=lambda: True,
        event_writer=lambda: writer,
        officer_config=lambda: None,
        session_type=type(session),
        idle_timeout_error=TimeoutError,
        stop_interrupt_watcher=AsyncMock(),
        stop_control_watcher=AsyncMock(),
        retire_announced_permissions=AsyncMock(),
    )
    owner = SessionTerminationCoordinator(
        SessionTerminationPorts(**values),
        logger=logging.getLogger(__name__),
        termination_queue_sentinel=object(),
    )
    side = owner.track_session_side_task(asyncio.create_task(side_work()))
    await entered.wait()
    teardown = asyncio.create_task(owner._terminate_inner("handoff", mark_thread=False))
    try:
        await asyncio.wait_for(cancelling.wait(), 1)
        writer.close.assert_not_awaited()
        session.cleanup.assert_not_awaited()
        identity.release_thread.assert_not_called()
        release.set()
        await asyncio.wait_for(teardown, 1)
        assert side.done()
        writer.close.assert_awaited_once()
        identity.release_thread.assert_called_once()
    finally:
        release.set()
        side.cancel()
        await asyncio.gather(side, teardown, return_exceptions=True)
