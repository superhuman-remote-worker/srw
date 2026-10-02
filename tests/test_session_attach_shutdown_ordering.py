"""A local cancellation deadline cannot transfer an attach's cleanup ownership."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from agent.api import persistent_app as pa


@pytest.fixture
def runtime(monkeypatch):
    for name in ("_session_identity", "_session_attach", "_session_termination"):
        original = getattr(pa, name)
        kwargs = (
            {
                "logger": original._logger,
                "termination_queue_sentinel": original.termination_queue_sentinel,
            }
            if name == "_session_termination"
            else {}
        )
        monkeypatch.setattr(pa, name, type(original)(original._ports, **kwargs))
    for name in ("_session", "_agent", "_orchestrator_client", "_heartbeat_task"):
        monkeypatch.setattr(pa, name, None)
    monkeypatch.setattr(pa, "_stateless_mode", lambda: False)
    monkeypatch.setattr(pa, "_app_guide_health", lambda: {"state": "ready"})
    client = MagicMock()
    for name in ("connect", "register", "deregister", "close"):
        setattr(client, name, AsyncMock(return_value=True))

    async def heartbeat(**kwargs):
        await asyncio.Future()

    client.run_heartbeat_loop = heartbeat
    agent = MagicMock(config=SimpleNamespace(agent_id=str(uuid4())))
    agent.initialize = AsyncMock()
    agent.shutdown = AsyncMock()
    monkeypatch.setattr(pa.UniversalAgent, "from_config", lambda _: agent)
    monkeypatch.setattr(pa, "create_orchestrator_client_from_env", lambda _: client)
    monkeypatch.setattr(pa._session_termination, "terminate", AsyncMock())
    monkeypatch.setattr(pa._session_attach, "release_shutdown_receipt", AsyncMock())
    return client


@pytest.mark.asyncio
@pytest.mark.parametrize("dedicated", [True, False], ids=["dedicated", "pool"])
@pytest.mark.parametrize("cleanup_blocked", [True, False], ids=["unproven", "joined"])
async def test_lifespan_never_terminates_over_attach_cleanup(
    monkeypatch, runtime, dedicated, cleanup_blocked
):
    entered, cleanup, finish = (asyncio.Event() for _ in range(3))
    thread = str(uuid4())

    async def attach(_thread):
        pa._session = SimpleNamespace()
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cleanup.set()
            if cleanup_blocked:
                await finish.wait()
            raise

    monkeypatch.setattr(pa._session_attach, "attach", attach)
    stop = pa._session_attach.stop_startup_attach

    async def bounded_stop():
        return await stop(cleanup_timeout=0.01)

    monkeypatch.setattr(pa._session_attach, "stop_startup_attach", bounded_stop)
    app = pa.create_persistent_app(
        "session_base", thread_id=thread if dedicated else None
    )
    context = pa.lifespan(app)
    await context.__aenter__()
    if not dedicated:
        pa._session_attach._pool_task = asyncio.create_task(attach(thread))
    await asyncio.wait_for(entered.wait(), 1)
    task = (
        pa._session_attach.startup_task if dedicated else pa._session_attach.pool_task
    )
    shutdown = asyncio.create_task(context.__aexit__(None, None, None))
    try:
        await asyncio.wait_for(asyncio.shield(shutdown), 0.25)
        assert cleanup.is_set()
        if cleanup_blocked:
            assert not task.done()
            pa._session_termination.terminate.assert_not_awaited()
            pa._session_attach.release_shutdown_receipt.assert_not_awaited()
        else:
            assert task.done()
            pa._session_termination.terminate.assert_awaited_once_with(
                "shutdown", mark_thread=True
            )
            pa._session_attach.release_shutdown_receipt.assert_awaited_once()
        runtime.close.assert_awaited_once()
    finally:
        finish.set()
        await asyncio.gather(task, shutdown, return_exceptions=True)
