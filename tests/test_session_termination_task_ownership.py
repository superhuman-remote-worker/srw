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
