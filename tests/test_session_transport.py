"""Outbound session delivery must not acquire journal or loop ownership."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

import agent.api.persistent_app as app
from agent.api import session_transport, session_websocket


def test_session_socket_keeps_its_idle_ping_policy():
    # Shorter than Cockpit's control-socket watchdog and edge idle timeouts.
    assert session_websocket.WS_PING_INTERVAL_S == 20.0


@pytest.mark.asyncio
async def test_idle_ping_then_queued_frame_are_direct_and_send_failure_stops_pump(
    monkeypatch,
):
    journal = Mock(side_effect=AssertionError("Direct pump output entered the journal"))
    monkeypatch.setattr(app, "_broadcast_frame", journal)
    queue = asyncio.Queue()
    frame = {"method": "token", "params": {"content": "hello", "_seq": [4, 7]}}
    sent = []

    async def send(payload):
        sent.append(payload)
        if len(sent) == 1:
            queue.put_nowait(frame)
        else:
            raise OSError("closed socket")

    ws = Mock(send_json=AsyncMock(side_effect=send))
    await asyncio.wait_for(
        session_transport.run_subscriber_pump(ws, queue, ping_interval=0.005),
        timeout=2,
    )
    assert sent == [{"method": "ws.ping", "params": {}}, frame]
    assert sent[1] is frame
    journal.assert_not_called()
    assert queue.empty()


@pytest.mark.asyncio
async def test_cancelled_send_is_joined_without_unsubscribing_or_cancelling_loop(
    monkeypatch,
):
    queue = asyncio.Queue()
    queue.put_nowait({"method": "first", "params": {}})
    queue.put_nowait({"method": "second", "params": {}})
    subscribers = {"client": queue}
    loop_owner = Mock()
    monkeypatch.setattr(app, "_subscribers", subscribers)
    monkeypatch.setattr(app, "_loop_task", loop_owner)
    sending, settled, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def send(payload):
        sending.set()
        try:
            await release.wait()
        finally:
            settled.set()

    ws = Mock(send_json=AsyncMock(side_effect=send))
    pump = asyncio.create_task(
        session_transport.run_subscriber_pump(
            ws, queue, ping_interval=session_websocket.WS_PING_INTERVAL_S
        )
    )
    try:
        await asyncio.wait_for(sending.wait(), timeout=2)
        pump.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pump, timeout=2)
        assert settled.is_set()
        assert subscribers == {"client": queue}
        assert app._subscribers is subscribers
        assert app._loop_task is loop_owner
        assert queue.get_nowait()["method"] == "second"
        loop_owner.assert_not_called()
        assert loop_owner.mock_calls == []
    finally:
        release.set()
        if not pump.done():
            pump.cancel()
        await asyncio.gather(pump, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_idle_pump_joins_queue_get_without_owning_lifecycle(
    monkeypatch,
):
    entered, settled = asyncio.Event(), asyncio.Event()
    getters = []

    class ObservedQueue(asyncio.Queue):
        async def get(self):
            getters.append(asyncio.current_task())
            entered.set()
            try:
                return await super().get()
            finally:
                settled.set()

    queue = ObservedQueue()
    subscribers = {"idle": queue}
    loop_owner = Mock()
    monkeypatch.setattr(app, "_subscribers", subscribers)
    monkeypatch.setattr(app, "_loop_task", loop_owner)
    ws = Mock(send_json=AsyncMock())
    pump = asyncio.create_task(
        session_transport.run_subscriber_pump(
            ws, queue, ping_interval=session_websocket.WS_PING_INTERVAL_S
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), timeout=2)
        pump.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(pump, timeout=2)
        assert settled.is_set()
        assert all(task.done() for task in getters)
        ws.send_json.assert_not_awaited()
        assert subscribers == {"idle": queue}
        assert app._subscribers is subscribers
        assert app._loop_task is loop_owner
        assert loop_owner.mock_calls == []
    finally:
        for task in [pump, *getters]:
            if not task.done():
                task.cancel()
        await asyncio.gather(pump, *getters, return_exceptions=True)


@pytest.mark.asyncio
async def test_direct_send_propagates_cancellation():
    cancellation = asyncio.CancelledError("direct sender was cancelled")
    ws = Mock(send_json=AsyncMock(side_effect=cancellation))
    with pytest.raises(asyncio.CancelledError) as caught:
        await app._ws_send(ws, "control.ack", {"accepted": True})
    assert caught.value is cancellation
    ws.send_json.assert_awaited_once_with(
        {"method": "control.ack", "params": {"accepted": True}}
    )


@pytest.mark.asyncio
async def test_slow_and_closed_subscriber_cannot_block_another_client(monkeypatch):
    monkeypatch.setattr(app, "_subscribers", {})
    monkeypatch.setattr(app, "_orchestrator_client", None)
    monkeypatch.setattr(app, "_SUBSCRIBER_QUEUE_MAXSIZE", 2)
    slow = app._subscribe("slow")
    fast = app._subscribe("fast")
    old = {"method": "old", "params": {}}
    middle = {"method": "middle", "params": {}}
    latest = {"method": "latest", "params": {}}
    app._fan_out_live_frame(old)
    app._fan_out_live_frame(middle)
    assert fast.get_nowait() is old
    assert fast.get_nowait() is middle
    app._fan_out_live_frame(latest)
    assert slow.get_nowait() is middle
    assert slow.get_nowait() is latest
    assert fast.get_nowait() is latest

    app._unsubscribe("slow")
    app._unsubscribe("slow")
    app._fan_out_live_frame(old)
    assert slow.empty()
    assert fast.get_nowait() is old
    assert app._subscribers == {"fast": fast}
